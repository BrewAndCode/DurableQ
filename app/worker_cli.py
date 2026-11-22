"""Environment-configured command-line entry point for the DurableQ worker."""

import logging
import os
import socket
from uuid import uuid4

from app.handlers import HANDLERS
from app.worker import Worker, run_worker_with_signals


def create_worker() -> Worker:
    """Construct a worker from supported Worker options in the environment."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured")

    worker_prefix = os.getenv("WORKER_ID", socket.gethostname()).strip()
    if not worker_prefix:
        raise ValueError("WORKER_ID must not be empty")

    return Worker(
        worker_id=f"{worker_prefix}-{uuid4().hex}",
        handlers=HANDLERS,
        poll_interval_seconds=float(os.getenv("POLL_INTERVAL_SECONDS", "1")),
        visibility_timeout_seconds=int(
            os.getenv("VISIBILITY_TIMEOUT_SECONDS", "60")
        ),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    run_worker_with_signals(create_worker())


if __name__ == "__main__":
    main()
