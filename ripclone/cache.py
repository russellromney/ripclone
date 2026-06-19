"""Local disk cache for tarballs, metadata packs, and refs."""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import config


def _repo_dir(owner: str, repo: str) -> Path:
    path = config.CACHE_DIR / owner / repo
    path.mkdir(parents=True, exist_ok=True)
    return path


def _ref_file(owner: str, repo: str, branch: str) -> Path:
    path = _repo_dir(owner, repo) / "refs"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{branch}.json"


def _tarball_file(owner: str, repo: str, commit: str) -> Path:
    path = _repo_dir(owner, repo) / "tarballs"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{commit}.tar.gz"


def _metadata_file(owner: str, repo: str, commit: str) -> Path:
    path = _repo_dir(owner, repo) / "metadata"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{commit}.pack"


def get_cached_ref(owner: str, repo: str, branch: str) -> Optional[tuple[str, datetime]]:
    ref_file = _ref_file(owner, repo, branch)
    if not ref_file.exists():
        return None
    data = json.loads(ref_file.read_text())
    sha = data.get("sha")
    cached_at = datetime.fromisoformat(data.get("cached_at", "1970-01-01T00:00:00+00:00"))
    return sha, cached_at


def set_cached_ref(owner: str, repo: str, branch: str, sha: str) -> None:
    ref_file = _ref_file(owner, repo, branch)
    data = {
        "sha": sha,
        "cached_at": datetime.now(timezone.utc).isoformat(),
    }
    ref_file.write_text(json.dumps(data))


def tarball_exists(owner: str, repo: str, commit: str) -> bool:
    return _tarball_file(owner, repo, commit).exists()


def metadata_exists(owner: str, repo: str, commit: str) -> bool:
    return _metadata_file(owner, repo, commit).exists()


def tarball_path(owner: str, repo: str, commit: str) -> Path:
    return _tarball_file(owner, repo, commit)


def metadata_path(owner: str, repo: str, commit: str) -> Path:
    return _metadata_file(owner, repo, commit)


def store_tarball(owner: str, repo: str, commit: str, source: Path) -> Path:
    dest = _tarball_file(owner, repo, commit)
    shutil.copy2(source, dest)
    return dest


def store_metadata(owner: str, repo: str, commit: str, source: Path) -> Path:
    dest = _metadata_file(owner, repo, commit)
    shutil.copy2(source, dest)
    return dest


def clear_ref(owner: str, repo: str, branch: str) -> None:
    ref_file = _ref_file(owner, repo, branch)
    if ref_file.exists():
        ref_file.unlink()
