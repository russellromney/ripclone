"""CLI for ripclone."""

import argparse
import base64
import json
import os
import secrets
import subprocess
import sys
import tempfile
import webbrowser
from pathlib import Path
from urllib.parse import urljoin

import httpx

DEFAULT_SERVER = os.getenv("REPOLAYER_SERVER", "http://localhost:8000")
GITHUB_MANIFEST_API = "https://api.github.com/app-manifests"
DEFAULT_TIMEOUT = httpx.Timeout(60.0, connect=10.0)


def _run(
    cmd: list[str],
    check: bool = True,
    cwd: Path | None = None,
    input: bytes | None = None,
    binary: bool = False,
) -> str | bytes:
    """Run a subprocess command.

    If binary=True, returns raw bytes and accepts bytes input.
    Otherwise returns stripped text and accepts bytes input by round-tripping
    through latin-1 (preserves arbitrary byte sequences).
    """
    if binary:
        result = subprocess.run(
            cmd,
            check=check,
            capture_output=True,
            cwd=cwd,
            input=input,
        )
        return result.stdout

    result = subprocess.run(
        cmd,
        check=check,
        capture_output=True,
        text=True,
        encoding="latin-1",
        cwd=cwd,
        input=input.decode("latin-1") if input is not None else None,
    )
    return result.stdout.strip()


def _parse_repo_arg(repo_arg: str) -> tuple[str, str]:
    if repo_arg.startswith("https://github.com/"):
        path = repo_arg[len("https://github.com/"):]
        path = path.removesuffix(".git")
        owner, repo = path.split("/", 1)
        return owner, repo
    if "/" not in repo_arg:
        raise ValueError("repo must be owner/name or a GitHub URL")
    owner, repo = repo_arg.split("/", 1)
    return owner, repo


def _get_current_branch() -> str:
    return _run(["git", "rev-parse", "--abbrev-ref", "HEAD"])


def _get_current_commit() -> str:
    return _run(["git", "rev-parse", "HEAD"])


def cmd_clone(args: argparse.Namespace) -> None:
    owner, repo = _parse_repo_arg(args.repo)
    branch = args.branch or _guess_default_branch(owner, repo) or "main"

    body = {
        "repo": f"{owner}/{repo}",
        "branch": branch,
    }
    if args.strict:
        body["staleness"] = 0

    resp = httpx.post(urljoin(args.server, "/v1/clone"), json=body, timeout=DEFAULT_TIMEOUT)
    if resp.status_code != 200:
        print(f"clone failed: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    target_dir = Path(args.dir or repo)
    if target_dir.exists():
        print(f"target directory already exists: {target_dir}", file=sys.stderr)
        sys.exit(1)
    target_dir.mkdir(parents=True)

    # Download and extract tarball.
    tarball_resp = httpx.get(data["tarball_url"], timeout=DEFAULT_TIMEOUT)
    tarball_resp.raise_for_status()
    _run(
        ["tar", "-xzf", "-", "--strip-components=1"],
        cwd=target_dir,
        input=tarball_resp.content,
    )

    # Initialize git and load metadata pack.
    _run(["git", "init", "-q"], cwd=target_dir)
    # GitHub tarballs don't preserve modes/symlinks; ignore those diffs.
    _run(["git", "config", "core.fileMode", "false"], cwd=target_dir)
    _run(["git", "config", "core.symlinks", "false"], cwd=target_dir)
    _run(
        ["git", "remote", "add", "origin", f"https://github.com/{owner}/{repo}.git"],
        cwd=target_dir,
    )
    metadata_resp = httpx.get(data["metadata_url"], timeout=DEFAULT_TIMEOUT)
    metadata_resp.raise_for_status()
    _run(
        ["git", "unpack-objects"],
        cwd=target_dir,
        input=metadata_resp.content,
    )
    _run(["git", "update-ref", "HEAD", data["commit"]], cwd=target_dir)
    # Mark the commit as shallow so git log/status don't chase missing parents.
    (target_dir / ".git" / "shallow").write_text(data["commit"] + "\n")
    _run(["git", "read-tree", "HEAD"], cwd=target_dir)

    print(f"Cloned into {target_dir} at {data['commit']}")


def _guess_default_branch(owner: str, repo: str) -> str | None:
    # Try HEAD symref via git ls-remote; if that fails, return None.
    try:
        out = _run(["git", "ls-remote", "--symref", f"https://github.com/{owner}/{repo}.git", "HEAD"])
        for line in out.splitlines():
            if line.startswith("ref: refs/heads/"):
                return line[len("ref: refs/heads/"):].split("\t")[0]
    except subprocess.CalledProcessError:
        pass
    return None


def cmd_commit(args: argparse.Namespace) -> None:
    if args.all:
        _run(["git", "add", "-A"])

    branch = _get_current_branch()
    expected_commit = _get_current_commit()

    # Build tree and commit without fetching old blobs.
    tree_sha = _run(["git", "write-tree", "--missing-ok"])
    commit_sha = _run(
        ["git", "commit-tree", tree_sha, "-p", "HEAD", "-m", args.message]
    )

    # Collect changed files and their new blob contents.
    changed = _run(["git", "diff", "--cached", "--name-only"]).splitlines()
    new_blobs = []
    for path in changed:
        try:
            blob_sha = _run(["git", "rev-parse", f":{path}"])
        except subprocess.CalledProcessError:
            continue  # deleted file
        blob_data = _run(["git", "cat-file", "-p", blob_sha], check=False, binary=True)
        if not blob_data:
            continue
        new_blobs.append({
            "sha1": blob_sha,
            "object": base64.b64encode(blob_data).decode(),
        })

    # Serialize commit and tree objects.
    commit_obj = _run(["git", "cat-file", "-p", commit_sha])
    tree_obj = _run(["git", "cat-file", "-p", tree_sha])

    # Parse owner/repo from git remote.
    remote_url = _run(["git", "remote", "get-url", "origin"])
    owner, repo = _parse_repo_arg(remote_url)

    body = {
        "repo": f"{owner}/{repo}",
        "branch": branch,
        "expected_commit": expected_commit,
        "commit_object": base64.b64encode(commit_obj.encode("latin-1")).decode(),
        "tree_object": base64.b64encode(tree_obj.encode("latin-1")).decode(),
        "new_blobs": new_blobs,
    }

    resp = httpx.post(urljoin(args.server, "/v1/commit"), json=body, timeout=DEFAULT_TIMEOUT)
    if resp.status_code != 200:
        print(f"commit failed: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    new_commit = resp.json()["commit"]
    _run(["git", "update-ref", "HEAD", new_commit])
    print(f"[{new_commit[:7]}] {args.message}")




def cmd_register_github_app(args: argparse.Namespace) -> None:
    """Run a local server to complete the GitHub App Manifest flow."""
    from http.server import BaseHTTPRequestHandler, HTTPServer

    state = secrets.token_urlsafe(16)
    port = args.port
    callback_url = f"http://localhost:{port}/github/callback"

    manifest = {
        "name": args.app_name,
        "url": args.app_url,
        "hook_attributes": {
            "url": args.webhook_url,
            "active": True,
        },
        "redirect_url": callback_url,
        "default_permissions": {
            "contents": "write",
            "metadata": "read",
        },
        "default_events": ["push"],
        "public": False,
    }

    github_form_action = "https://github.com/settings/apps/new"
    if args.org:
        github_form_action = f"https://github.com/organizations/{args.org}/settings/apps/new"

    form_html = f"""<!doctype html>
<html>
<head><title>Create ripclone GitHub App</title></head>
<body>
<h1>Create ripclone GitHub App</h1>
<form action="{github_form_action}?state={state}" method="post">
  <input type="hidden" name="manifest" id="manifest">
  <p>Click the button below to create the GitHub App under your account.</p>
  <button type="submit">Create GitHub App</button>
</form>
<script>
document.getElementById('manifest').value = JSON.stringify({json.dumps(manifest)});
</script>
</body>
</html>"""

    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False) as f:
        f.write(form_html)
        form_path = f.name

    print(f"Local callback server listening on http://localhost:{port}")
    print(f"Open this file in your browser and click 'Create GitHub App':")
    print(f"  file://{form_path}")
    if args.open:
        webbrowser.open(f"file://{form_path}")

    received_code: list[str | None] = [None]
    received_state: list[str | None] = [None]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:
            pass

        def do_GET(self) -> None:
            from urllib.parse import parse_qs, urlparse
            parsed = urlparse(self.path)
            if parsed.path != "/github/callback":
                self.send_error(404)
                return
            qs = parse_qs(parsed.query)
            code = qs.get("code", [None])[0]
            returned_state = qs.get("state", [None])[0]
            if not code:
                self.send_error(400, "Missing code")
                return
            if returned_state != state:
                self.send_error(400, "State mismatch")
                return
            received_code[0] = code
            received_state[0] = returned_state
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"GitHub App registered. You can close this tab.")

    class ReusableHTTPServer(HTTPServer):
        allow_reuse_address = True

    server = ReusableHTTPServer(("127.0.0.1", port), Handler)
    server.timeout = 1
    print("Waiting for GitHub redirect...")
    while received_code[0] is None:
        server.handle_request()
    server.server_close()

    code = received_code[0]
    print(f"Exchanging temporary code for app credentials...")
    resp = httpx.post(f"{GITHUB_MANIFEST_API}/{code}/conversions")
    if resp.status_code not in (200, 201):
        print(f"Failed to exchange code: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    app_config = resp.json()
    app_id = str(app_config["id"])
    pem = app_config["pem"]
    webhook_secret = app_config["webhook_secret"]

    print(f"GitHub App created: ID {app_id}")

    # Save to soup secrets if requested.
    if not args.skip_soup:
        _run(["soup", "secrets", "set", "REPOLAYER_APP_ID", app_id])
        _run(["soup", "secrets", "set", "REPOLAYER_PRIVATE_KEY", pem])
        _run(["soup", "secrets", "set", "REPOLAYER_WEBHOOK_SECRET", webhook_secret])
        print("Saved REPOLAYER_APP_ID, REPOLAYER_PRIVATE_KEY, and REPOLAYER_WEBHOOK_SECRET to soup.")

    print("\nNext steps:")
    print("1. Install the app on the repo/org you want to use.")
    print(f"   Install URL: {app_config.get('html_url', 'https://github.com/settings/apps/' + args.app_name + '/installations')}")
    print("2. Deploy ripclone with these secrets.")


def main() -> None:
    parser = argparse.ArgumentParser(description="ripclone CLI")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="ripclone server URL")
    sub = parser.add_subparsers(dest="command", required=True)

    clone_p = sub.add_parser("clone", help="clone a GitHub repo")
    clone_p.add_argument("repo", help="owner/name or GitHub URL")
    clone_p.add_argument("--branch", "-b", help="branch to clone")
    clone_p.add_argument("--dir", "-d", help="destination directory")
    clone_p.add_argument("--strict", action="store_true", help="verify ref freshness")
    clone_p.set_defaults(func=cmd_clone)

    commit_p = sub.add_parser("commit", help="commit staged changes")
    commit_p.add_argument("-m", "--message", required=True, help="commit message")
    commit_p.add_argument("-a", "--all", action="store_true", help="stage all changes first")
    commit_p.set_defaults(func=cmd_commit)

    register_p = sub.add_parser("register-github-app", help="create a GitHub App via manifest flow")
    register_p.add_argument("--app-name", default="ripclone", help="GitHub App name")
    register_p.add_argument("--app-url", default="https://ripclone.fly.dev", help="App homepage URL")
    register_p.add_argument("--webhook-url", default="https://ripclone.fly.dev/v1/github/webhook", help="Webhook URL")
    register_p.add_argument("--org", help="Create under an organization instead of personal account")
    register_p.add_argument("--port", type=int, default=8123, help="Local callback port")
    register_p.add_argument("--open", action="store_true", help="Open the form in a browser automatically")
    register_p.add_argument("--skip-soup", action="store_true", help="Don't save secrets to soup")
    register_p.set_defaults(func=cmd_register_github_app)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
