"""Entry point: python -m ripclone"""

import os

import uvicorn

from . import config

if __name__ == "__main__":
    config.validate()
    workers = int(os.getenv("REPOLAYER_WORKERS", "1"))
    uvicorn.run(
        "ripclone.server:app",
        host=config.HOST,
        port=config.PORT,
        log_level=config.LOG_LEVEL,
        reload=False,
        workers=workers,
    )
