"""FastAPI server for ripclone."""

import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from . import cache, commit, config, github, sync

app = FastAPI(title="ripclone")

# Limit concurrent cache-warming jobs so we don't OOM or saturate the volume.
_WARM_SEMAPHORE = asyncio.Semaphore(int(os.getenv("REPOLAYER_WARM_CONCURRENCY", "2")))


def _run(cmd: list[str], cwd: Path, check: bool = True) -> str:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _public_base_url(request: Request) -> str:
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get(
        "x-forwarded-host", request.headers.get("host", request.url.hostname)
    )
    return f"{scheme}://{host}"


@app.post("/v1/clone")
async def clone_repo(request: Request, body: dict):
    owner_repo = body.get("repo", "")
    if "/" not in owner_repo:
        raise HTTPException(status_code=400, detail="repo must be owner/name")
    owner, repo = owner_repo.split("/", 1)
    branch = body.get("branch", "main")
    staleness = body.get("staleness", config.DEFAULT_STALENESS)

    token = await github.get_installation_token(owner)

    if not await github.check_repo_access(owner, repo, token):
        raise HTTPException(
            status_code=403,
            detail="No read access to repo (public repos work without an installation)",
        )

    # Decide if we need to refresh the ref.
    cached = cache.get_cached_ref(owner, repo, branch)
    commit_sha: str | None = None
    now = datetime.now(timezone.utc)
    if cached:
        commit_sha, cached_at = cached
        age = (now - cached_at).total_seconds()
        if age > staleness:
            commit_sha = None
    if not commit_sha:
        commit_sha = await github.get_ref(owner, repo, f"heads/{branch}", token)
        if not commit_sha:
            raise HTTPException(status_code=404, detail="Branch not found")
        cache.set_cached_ref(owner, repo, branch, commit_sha)
        cached_at = now

    fresh_until = cached_at + timedelta(seconds=staleness)

    # Ensure tarball and metadata exist.
    tarball_path = cache.tarball_path(owner, repo, commit_sha)
    metadata_path = cache.metadata_path(owner, repo, commit_sha)

    if not tarball_path.exists() or not metadata_path.exists():
        async with _WARM_SEMAPHORE:
            await _warm_cache(owner, repo, commit_sha, token)

    base_url = _public_base_url(request)
    return {
        "ref": f"refs/heads/{branch}",
        "commit": commit_sha,
        "tarball_url": f"{base_url}/cache/{owner}/{repo}/tarballs/{commit_sha}.tar.gz",
        "metadata_url": f"{base_url}/cache/{owner}/{repo}/metadata/{commit_sha}.pack",
        "cached_at": cached_at.isoformat(),
        "fresh_until": fresh_until.isoformat(),
    }


async def _warm_cache(owner: str, repo: str, commit_sha: str, token: str | None) -> None:
    """Stream GitHub tarball and commit+tree metadata into the local cache."""
    tarball_url = (
        f"https://github.com/{owner}/{repo}/archive/{commit_sha}.tar.gz"
    )
    tarball_dest = cache.tarball_path(owner, repo, commit_sha)
    metadata_dest = cache.metadata_path(owner, repo, commit_sha)

    # Stream-download tarball so we don't buffer the whole thing in memory.
    if not tarball_dest.exists():
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
            async with client.stream("GET", tarball_url, follow_redirects=True) as resp:
                if resp.status_code != 200:
                    raise HTTPException(
                        status_code=502,
                        detail=f"GitHub tarball fetch failed: {resp.status_code}",
                    )
                with tarball_dest.open("wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        f.write(chunk)

    # Fetch commit + tree via git protocol.
    if not metadata_dest.exists():
        with tempfile.TemporaryDirectory(prefix="ripclone-clone-") as tmp:
            repo_dir = Path(tmp)
            _run(["git", "init", "-q"], cwd=repo_dir)
            remote = github.git_remote_url(owner, repo, token)
            _run(["git", "remote", "add", "origin", remote], cwd=repo_dir)
            _run(
                [
                    "git",
                    "fetch",
                    "--depth=1",
                    "--filter=blob:none",
                    "origin",
                    commit_sha,
                ],
                cwd=repo_dir,
            )
            pack_dir = repo_dir / ".git" / "objects" / "pack"
            packs = list(pack_dir.glob("*.pack"))
            if not packs:
                raise HTTPException(status_code=500, detail="No packfile produced")
            # Copy the packfile without reading it fully into memory.
            shutil.copyfile(packs[0], metadata_dest)


@app.post("/v1/commit")
async def create_commit(body: dict):
    owner_repo = body.get("repo", "")
    if "/" not in owner_repo:
        raise HTTPException(status_code=400, detail="repo must be owner/name")
    owner, repo = owner_repo.split("/", 1)
    branch = body.get("branch", "main")

    try:
        new_sha = await commit.push_commit(
            owner=owner,
            repo=repo,
            branch=branch,
            expected_commit=body.get("expected_commit", ""),
            commit_object_b64=body.get("commit_object", ""),
            tree_object_b64=body.get("tree_object", ""),
            new_blobs_b64=body.get("new_blobs", []),
        )
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))

    # Invalidate cached ref so the next clone fetches the new commit.
    cache.clear_ref(owner, repo, branch)

    return {"commit": new_sha}


@app.post("/v1/github/webhook")
async def github_webhook(request: Request, x_hub_signature_256: str = Header(default="")):
    payload = await request.body()
    if not sync.verify_webhook_signature(payload, x_hub_signature_256):
        raise HTTPException(status_code=401, detail="Invalid signature")
    data = json.loads(payload)
    if data.get("ref"):
        sync.handle_push_event(data)
    return {"ok": True}


@app.get("/cache/{owner}/{repo}/tarballs/{commit}.tar.gz")
async def serve_tarball(owner: str, repo: str, commit: str):
    path = cache.tarball_path(owner, repo, commit)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Tarball not found")
    return FileResponse(path)


@app.get("/cache/{owner}/{repo}/metadata/{commit}.pack")
async def serve_metadata(owner: str, repo: str, commit: str):
    path = cache.metadata_path(owner, repo, commit)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Metadata not found")
    return FileResponse(path)


@app.post("/v1/sync/{owner}/{repo}/{branch}")
async def sync_ref(owner: str, repo: str, branch: str):
    """Force a refresh of a cached ref from GitHub."""
    token = await github.get_installation_token(owner)
    if not token:
        raise HTTPException(
            status_code=401,
            detail=f"No GitHub App installation found for {owner}",
        )
    commit_sha = await github.get_ref(owner, repo, f"heads/{branch}", token)
    if not commit_sha:
        raise HTTPException(status_code=404, detail="Branch not found")
    cache.set_cached_ref(owner, repo, branch, commit_sha)
    return {"ref": f"refs/heads/{branch}", "commit": commit_sha}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
