from loguru import logger
from logtail import LogtailHandler
import os
import sys

def setup_logging():
    source_token = os.getenv("BETTERSTACK_SOURCE_TOKEN_PYTHON")
    ingest_host  = os.getenv("BETTERSTACK_INGEST_HOST_PYTHON")

    # If no token → skip BetterStack integration (local dev)
    if not source_token:
        logger.warning("Better Stack logging disabled")
        return

    handler = LogtailHandler(
        source_token=source_token,
        host=ingest_host
    )

    logger.remove()  # remove default stderr
    logger.add(
        sys.stderr,
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="{time} | {level} | {message} | {extra}",
        serialize=False,  # human-readable console format
    )
    logger.add(
            handler,
            level=os.getenv("BETTERSTACK_LOG_LEVEL", "INFO"),
            serialize=True,  # JSON output for ingestion
            filter=None
        )

    logger.info("Better Stack logging initialized")
