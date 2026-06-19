"""Commit validation and push to GitHub."""

import base64
import os
import subprocess
import tempfile
from pathlib import Path

from . import github


def _run(
    cmd: list[str],
    cwd: Path,
    check: bool = True,
    input: bytes | None = None,
) -> str:
    """Run a subprocess command, accepting binary input via latin-1 round-trip."""
    result = subprocess.run(
        cmd,
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
        encoding="latin-1",
        input=input.decode("latin-1") if input is not None else None,
    )
    return result.stdout.strip()


def _write_object(repo_dir: Path, obj_type: str, data: bytes) -> str:
    """Write a git object and return its sha."""
    sha = _run(
        ["git", "hash-object", "-t", obj_type, "-w", "--stdin"],
        cwd=repo_dir,
        input=data,
    )
    return sha


async def push_commit(
    owner: str,
    repo: str,
    branch: str,
    expected_commit: str,
    commit_object_b64: str,
    tree_object_b64: str,
    new_blobs_b64: list[dict],
) -> str:
    """Validate and push a commit to GitHub.

    Returns the new commit SHA.
    Raises RuntimeError on validation or push failure.
    """
    token = await github.get_installation_token(owner)
    if not token:
        raise RuntimeError(f"No GitHub App installation found for {owner}")

    # Verify current ref matches expected_commit.
    current_sha = await github.get_ref(owner, repo, f"heads/{branch}", token)
    if current_sha != expected_commit:
        raise RuntimeError(
            f"Ref mismatch: expected {expected_commit}, GitHub has {current_sha}"
        )

    # Decode objects.
    commit_data = base64.b64decode(commit_object_b64)
    tree_data = base64.b64decode(tree_object_b64)
    new_blobs = [
        (b["sha1"], base64.b64decode(b["object"])) for b in new_blobs_b64
    ]

    with tempfile.TemporaryDirectory(prefix="ripclone-commit-") as tmp:
        repo_dir = Path(tmp)
        _run(["git", "init", "--bare", "-q"], cwd=repo_dir)

        # Fetch the parent commit shallowly so push negotiation knows what the
        # remote already has.
        remote = github.git_remote_url(owner, repo, token)
        _run(
            [
                "git",
                "fetch",
                "--depth=1",
                remote,
                expected_commit,
            ],
            cwd=repo_dir,
        )

        # Write new blobs.
        for expected_sha, blob_data in new_blobs:
            sha = _write_object(repo_dir, "blob", blob_data)
            if sha != expected_sha:
                raise RuntimeError(
                    f"Blob hash mismatch: expected {expected_sha}, got {sha}"
                )

        # Write tree and commit.
        tree_sha = _write_object(repo_dir, "tree", tree_data)
        commit_sha = _write_object(repo_dir, "commit", commit_data)

        # Verify commit parent and tree.
        commit_show = _run(["git", "cat-file", "-p", commit_sha], cwd=repo_dir)
        if f"tree {tree_sha}" not in commit_show:
            raise RuntimeError("Commit tree mismatch")
        if f"parent {expected_commit}" not in commit_show:
            raise RuntimeError("Commit parent mismatch")

        # Push.
        _run(
            ["git", "push", remote, f"{commit_sha}:refs/heads/{branch}"],
            cwd=repo_dir,
        )

        return commit_sha
