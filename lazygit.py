#!/usr/bin/env python3
"""
lazygit - fast "agent-style" clone/commit for GitHub repos.

Optimized for the coding-agent workflow: you want the current files of a
branch, you edit them, then you commit and push. History is irrelevant, so
we avoid downloading it.

Strategy:
  1. Download the branch tarball from GitHub's CDN (fast, single HTTP req).
  2. Extract it as the working tree.
  3. Fetch only the commit + tree objects from git (no blobs) via
     --depth=1 --filter=blob:none.
  4. Reconstruct the index from HEAD's tree so `git status` works.

Committing without pulling old blobs:
  - Stage your changes with normal `git add`.
  - `lazygit commit -m "msg"` runs `git write-tree --missing-ok`, which
    writes the new tree without requiring old blob contents, then builds
    the commit object on top of HEAD.

This keeps the initial .git directory tiny (~hundreds of KB) and avoids
fetching megabytes of blob history just to make a new commit.
"""

import argparse
import concurrent.futures
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


def run(cmd, **kwargs):
    """Run a shell command and return stripped stdout."""
    kwargs.setdefault("check", True)
    kwargs.setdefault("text", True)
    kwargs.setdefault("capture_output", True)
    return subprocess.run(cmd, **kwargs).stdout.strip()


def parse_github_url(url: str):
    """Extract (owner, repo) from https or ssh GitHub URLs."""
    patterns = [
        r"https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$",
        r"git@github\.com:([^/]+)/([^/]+?)(?:\.git)?$",
    ]
    for pat in patterns:
        m = re.match(pat, url)
        if m:
            return m.group(1), m.group(2)
    raise ValueError(f"Only GitHub URLs are supported. Got: {url}")


def ensure_author():
    """Make sure author/committer env vars are set so commit-tree works."""
    defaults = {
        "GIT_AUTHOR_NAME": "lazygit",
        "GIT_AUTHOR_EMAIL": "lazygit@localhost",
        "GIT_COMMITTER_NAME": "lazygit",
        "GIT_COMMITTER_EMAIL": "lazygit@localhost",
    }
    for key, val in defaults.items():
        if not os.environ.get(key):
            os.environ[key] = val


def cmd_clone(args):
    owner, repo = parse_github_url(args.url)
    branch = args.branch
    target = Path(args.dir or repo).resolve()

    if target.exists():
        print(f"error: target directory already exists: {target}", file=sys.stderr)
        sys.exit(1)

    target.mkdir(parents=True)

    # Set up a minimal git repo early so we can detect the default branch.
    run(["git", "init", "-q"], cwd=target)
    run(["git", "remote", "add", "origin", args.url], cwd=target)

    if branch is None:
        symref = run(["git", "ls-remote", "--symref", "origin", "HEAD"], cwd=target)
        # Output looks like: ref: refs/heads/main\tHEAD
        m = re.search(r"ref: refs/heads/(\S+)", symref)
        if not m:
            print("error: could not detect default branch", file=sys.stderr)
            sys.exit(1)
        branch = m.group(1)

    tarball_url = f"https://github.com/{owner}/{repo}/archive/refs/heads/{branch}.tar.gz"
    print(f"Cloning {owner}/{repo}@{branch} into {target}")

    tarball_path = tempfile.mktemp(suffix=".tar.gz")

    def download_tarball():
        print("  downloading tarball...")
        run(
            ["curl", "-fsSL", "-o", tarball_path, tarball_url],
            cwd=target,
        )
        return tarball_path

    def fetch_git_metadata():
        print("  fetching commit/tree metadata...")
        run(
            ["git", "fetch", "--depth=1", "--filter=blob:none", "origin", branch],
            cwd=target,
        )

    try:
        # Run tarball download and git metadata fetch in parallel.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            tarball_future = pool.submit(download_tarball)
            metadata_future = pool.submit(fetch_git_metadata)
            tarball_future.result()
            metadata_future.result()

        print("  extracting tarball...")
        run(
            ["tar", "-xzf", tarball_path, "--strip-components=1"],
            cwd=target,
        )
    finally:
        if os.path.exists(tarball_path):
            os.unlink(tarball_path)

    # Wire HEAD to a local branch tracking origin and populate the index.
    run(["git", "update-ref", f"refs/heads/{branch}", "FETCH_HEAD"], cwd=target)
    run(["git", "symbolic-ref", "HEAD", f"refs/heads/{branch}"], cwd=target)
    run(["git", "branch", "-u", f"origin/{branch}", branch, "-q"], cwd=target)
    run(["git", "read-tree", "HEAD"], cwd=target)

    git_size = run(["du", "-sh", str(target / ".git")], cwd=target).split()[0]
    print(f"  done. .git size: {git_size}")
    print(f"  run: cd {target}")


def cmd_commit(args):
    target = Path(args.dir).resolve()
    if not (target / ".git").is_dir():
        print("error: not a lazygit repository", file=sys.stderr)
        sys.exit(1)

    ensure_author()

    if args.all:
        run(["git", "add", "-A"], cwd=target)

    # Build the new tree without fetching missing (old) blob contents.
    tree = run(["git", "write-tree", "--missing-ok"], cwd=target)
    commit = run(
        ["git", "commit-tree", tree, "-p", "HEAD", "-m", args.message],
        cwd=target,
    )
    run(["git", "update-ref", "HEAD", commit], cwd=target)
    print(f"[{commit[:7]}] {args.message}")


def cmd_push(args):
    target = Path(args.dir).resolve()
    run(["git", "push"] + args.git_push_args, cwd=target)


def main():
    parser = argparse.ArgumentParser(
        description="Fast agent-style git clone/commit for GitHub.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python lazygit.py clone https://github.com/oven-sh/bun.git --branch main
  cd bun
  # make edits...
  python ../lazygit.py commit -m "my change"
  python ../lazygit.py push
""",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    clone_p = sub.add_parser("clone", help="fast clone of a GitHub branch")
    clone_p.add_argument("url", help="GitHub https or ssh URL")
    clone_p.add_argument("--branch", "-b", default=None, help="branch to clone (default: remote default branch)")
    clone_p.add_argument("--dir", "-d", help="destination directory (default: repo name)")
    clone_p.set_defaults(func=cmd_clone)

    commit_p = sub.add_parser("commit", help="commit staged changes without pulling old blobs")
    commit_p.add_argument("-m", "--message", required=True, help="commit message")
    commit_p.add_argument("-a", "--all", action="store_true", help="stage all changes first")
    commit_p.add_argument("--dir", "-d", default=".", help="repository directory")
    commit_p.set_defaults(func=cmd_commit)

    push_p = sub.add_parser("push", help="push current branch")
    push_p.add_argument("--dir", "-d", default=".", help="repository directory")
    push_p.add_argument("git_push_args", nargs="*", help="extra args for git push")
    push_p.set_defaults(func=cmd_push)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
