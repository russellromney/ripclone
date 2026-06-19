"""GitHub App authentication and API helpers."""

import base64
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import jwt

from . import config

_GITHUB_API = "https://api.github.com"


def _load_private_key() -> str:
    path = config._ensure_private_key()
    return path.read_text()


def make_app_jwt() -> str:
    """Create a JWT for the GitHub App."""
    now = int(time.time())
    payload = {
        "iat": now - 60,
        "exp": now + 600,
        "iss": str(config.APP_ID),
    }
    return jwt.encode(payload, _load_private_key(), algorithm="RS256")


async def get_installation_token(owner: str) -> Optional[str]:
    """Find an installation for the account and get an access token."""
    app_jwt = make_app_jwt()
    async with httpx.AsyncClient(
        base_url=_GITHUB_API,
        headers={
            "Authorization": f"Bearer {app_jwt}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    ) as client:
        resp = await client.get("/app/installations")
        resp.raise_for_status()
        for installation in resp.json():
            acct = installation.get("account") or {}
            if acct.get("login") == owner:
                inst_id = installation["id"]
                token_resp = await client.post(
                    f"/app/installations/{inst_id}/access_tokens",
                    json={"permissions": {"contents": "write"}},
                )
                token_resp.raise_for_status()
                return token_resp.json()["token"]
    return None


def _api_headers(token: str | None) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def check_repo_access(owner: str, repo: str, token: str | None = None) -> bool:
    """Return True if the repo is readable (public or token has access)."""
    async with httpx.AsyncClient(
        base_url=_GITHUB_API,
        headers=_api_headers(token),
    ) as client:
        resp = await client.get(f"/repos/{owner}/{repo}")
        return resp.status_code == 200


async def get_ref(owner: str, repo: str, ref: str, token: str | None = None) -> Optional[str]:
    """Fetch the current SHA for a ref like 'heads/main'."""
    async with httpx.AsyncClient(
        base_url=_GITHUB_API,
        headers=_api_headers(token),
    ) as client:
        resp = await client.get(f"/repos/{owner}/{repo}/git/refs/{ref}")
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data.get("object", {}).get("sha")


def git_remote_url(owner: str, repo: str, token: str | None = None) -> str:
    """HTTPS git remote, with embedded token when available."""
    if token:
        return f"https://x-access-token:{token}@github.com/{owner}/{repo}.git"
    return f"https://github.com/{owner}/{repo}.git"
