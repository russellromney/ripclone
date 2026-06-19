"""Webhook handling and cache invalidation."""

import hashlib
import hmac

from . import cache, config


def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    """Verify a GitHub webhook signature."""
    if not config.WEBHOOK_SECRET:
        return True
    secret = config.WEBHOOK_SECRET.encode()
    expected = "sha256=" + hmac.new(secret, payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def handle_push_event(payload: dict) -> None:
    """Invalidate cached refs for a push event."""
    repo = payload.get("repository", {})
    full_name = repo.get("full_name", "")
    if "/" not in full_name:
        return
    owner, repo_name = full_name.split("/", 1)
    ref = payload.get("ref", "")
    if not ref.startswith("refs/heads/"):
        return
    branch = ref[len("refs/heads/"):]
    cache.clear_ref(owner, repo_name, branch)
