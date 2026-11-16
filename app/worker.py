import logging
import math
import signal
import threading
from collections.abc import Callable

from app.queue import claim, complete, fail, heartbeat

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

        if (
            isinstance(poll_interval_seconds, bool)
            or not isinstance(poll_interval_seconds, (int, float))
            or not math.isfinite(poll_interval_seconds)
            or poll_interval_seconds <= 0
        ):
            raise ValueError("poll_interval_seconds must be a positive finite number")

        if (
            isinstance(visibility_timeout_seconds, bool)
            or not isinstance(visibility_timeout_seconds, int)
            or visibility_timeout_seconds < 1
        ):
            raise ValueError("visibility_timeout_seconds must be a positive integer")

        self.worker_id = worker_id.strip()
        self.handlers = handlers
        self.poll_interval_seconds = poll_interval_seconds
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self.stop_event = threading.Event()

    def _heartbeat_loop(
        self,
        job_id: int,
        attempt: int,
        stop_event: threading.Event,
        lease_lost: threading.Event,
    ):
        interval = max(0.1, self.visibility_timeout_seconds / 3)

        while not stop_event.wait(interval):
            try:
                renewed = heartbeat(
                    job_id=job_id,
                    worker_id=self.worker_id,
                    attempt=attempt,
                    visibility_timeout_seconds=self.visibility_timeout_seconds,
                )

                if not renewed:
                    logger.warning("Lease lost for job %s", job_id)
                    lease_lost.set()
                    return

            except Exception:
                logger.exception("Heartbeat failed for job %s", job_id)
                lease_lost.set()
                return

    def run_once(self) -> bool:
        if self.stop_event.is_set():
            return False

        job = claim(
            worker_id=self.worker_id,
            visibility_timeout_seconds=self.visibility_timeout_seconds,
        )

        if job is None:
            return False

        job_id, job_type, payload, attempt, _ = job
        handler = self.handlers.get(job_type)

        heartbeat_stop = threading.Event()
        lease_lost = threading.Event()

        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(job_id, attempt, heartbeat_stop, lease_lost),
            daemon=True,
        )

        heartbeat_thread.start()
        handler_error = None

        try:
            if handler is None:
                raise ValueError(f"No handler registered for job type: {job_type}")

            handler(payload)

        except Exception as exc:
            handler_error = exc
            logger.exception("Job %s failed", job_id)

        finally:
            heartbeat_stop.set()
            heartbeat_thread.join()

        if lease_lost.is_set():
            logger.warning("Job %s may have lost its lease", job_id)

        if handler_error is not None:
            fail(
                job_id=job_id,
                worker_id=self.worker_id,
                attempt=attempt,
                error=str(handler_error),
            )
            return True

        completed = complete(
            job_id=job_id,
            worker_id=self.worker_id,
            attempt=attempt,
        )

        if not completed:
            logger.warning(
                "Job %s completion rejected (stale attempt)",
                job_id,
            )

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


def run_worker_with_signals(worker: Worker):
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("Signal handlers must be installed from the main thread")

    previous_handlers = {}

    def handle_shutdown(signum, frame):
        logger.info(
            "Received signal %s. Stopping worker...",
            signum,
        )
        worker.stop()

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, handle_shutdown)

        worker.run_forever()

    finally:
        for sig, previous_handler in previous_handlers.items():
            signal.signal(sig, previous_handler)
