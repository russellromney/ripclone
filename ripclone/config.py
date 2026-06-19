"""Configuration loaded from environment."""

import base64
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

APP_ID = os.getenv("REPOLAYER_APP_ID", "")
PRIVATE_KEY_PATH = os.getenv("REPOLAYER_PRIVATE_KEY_PATH", "")
PRIVATE_KEY = os.getenv("REPOLAYER_PRIVATE_KEY", "")
PRIVATE_KEY_B64 = os.getenv("REPOLAYER_PRIVATE_KEY_B64", "")
WEBHOOK_SECRET = os.getenv("REPOLAYER_WEBHOOK_SECRET", "")
HOST = os.getenv("REPOLAYER_HOST", "0.0.0.0")
PORT = int(os.getenv("REPOLAYER_PORT", "8000"))
CACHE_DIR = Path(os.getenv("REPOLAYER_CACHE_DIR", "./data/cache"))
LOG_LEVEL = os.getenv("REPOLAYER_LOG_LEVEL", "info")
DEFAULT_STALENESS = int(os.getenv("REPOLAYER_DEFAULT_STALENESS", "30"))


def _private_key_path() -> Path:
    """Return the resolved private key path, defaulting to /data/private-key.pem."""
    return Path(PRIVATE_KEY_PATH) if PRIVATE_KEY_PATH else Path("/data/private-key.pem")


def _ensure_private_key() -> Path:
    """Write PEM content to disk if supplied inline or base64-encoded."""
    path = _private_key_path()
    pem = PRIVATE_KEY
    if PRIVATE_KEY_B64:
        pem = base64.b64decode(PRIVATE_KEY_B64).decode()
    elif pem:
        # Handle PEMs that were escaped during transport.
        pem = pem.replace("\\n", "\n")
    if pem:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(pem)
    return path


def validate() -> None:
    if not APP_ID:
        raise RuntimeError("REPOLAYER_APP_ID is required")
    path = _ensure_private_key()
    if not path.exists():
        raise RuntimeError(
            f"Private key not found at {path}. "
            "Set REPOLAYER_PRIVATE_KEY (PEM content) or REPOLAYER_PRIVATE_KEY_PATH."
        )
