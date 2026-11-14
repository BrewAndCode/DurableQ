
import logging
import threading
import time
from collections.abc import Callable

from app.queue import claim, complete, fail

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        worker_id: str,
        handlers: dict[str, Callable],
        poll_interval_seconds: float = 1,
        visibility_timeout_seconds: int = 60,
    ):
        if not worker_id or not worker_id.strip():
            raise ValueError("worker_id must be non-empty")

        if not handlers:
            raise ValueError("At least one handler is required")

        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")

        self.worker_id = worker_id.strip()
        self.handlers = handlers
        self.poll_interval_seconds = poll_interval_seconds
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self.stop_event = threading.Event()

    def run_once(self) -> bool:
        job = claim(
            worker_id=self.worker_id,
            visibility_timeout_seconds=self.visibility_timeout_seconds,
        )

        if job is None:
            return False

        job_id, job_type, payload, attempt, _ = job

        handler = self.handlers.get(job_type)

        try:
            if handler is None:
                raise ValueError(f"No handler registered for job type: {job_type}")

            handler(payload)

        except Exception as exc:
            logger.exception("Job %s failed", job_id)

            fail(
                job_id=job_id,
                worker_id=self.worker_id,
                attempt=attempt,
                error=str(exc),
            )
            return True

        completed = complete(
            job_id=job_id,
            worker_id=self.worker_id,
            attempt=attempt,
        )

        if not completed:
            logger.warning("Job %s completion rejected (stale attempt)", job_id)

        return True

    def run_forever(self):
        logger.info("Worker %s started", self.worker_id)

        while not self.stop_event.is_set():
            try:
                processed = self.run_once()

                if not processed:
                    self.stop_event.wait(self.poll_interval_seconds)

            except Exception:
                logger.exception("Worker encountered an unexpected error")
                self.stop_event.wait(self.poll_interval_seconds)

        logger.info("Worker %s stopped", self.worker_id)

    def stop(self):
        self.stop_event.set()
