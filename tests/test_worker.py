import os
import selectors
import signal
import subprocess
import sys
import threading
import time
from uuid import uuid4

import pytest

from app.database import get_connection
from app.queue import enqueue
from app.worker import CancellableHandler, Worker, run_worker_with_signals


def _wait_for_stdout_marker(process, marker, timeout):
    if process.stdout is None:
        return False

    deadline = time.monotonic() + timeout
    output = ""
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while time.monotonic() < deadline:
            remaining = max(0, deadline - time.monotonic())
            if not selector.select(remaining):
                return False

            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                return False
            output += chunk.decode(errors="replace")
            if marker in output:
                return True
            if process.poll() is not None:
                return False

    return False


@pytest.fixture(autouse=True)
def clean_jobs_table():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")

    yield

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")


def test_worker_completes_successful_job(monkeypatch):
    completed_jobs = []
    executed_payloads = []

    def fake_claim(**kwargs):
        return (1, "send_email", {"user_id": 101}, 1, None)

    def fake_complete(**kwargs):
        completed_jobs.append(kwargs)
        return True

    def handler(payload):
        executed_payloads.append(payload)

    monkeypatch.setattr("app.worker.claim", fake_claim)
    monkeypatch.setattr("app.worker.complete", fake_complete)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": handler},
    )

    result = worker.run_once()

    assert result is True
    assert executed_payloads == [{"user_id": 101}]
    assert len(completed_jobs) == 1
    assert completed_jobs[0]["job_id"] == 1
    assert completed_jobs[0]["attempt"] == 1


def test_worker_fails_job_when_handler_raises(monkeypatch):
    failed_jobs = []
    completed_jobs = []

    def fake_claim(**kwargs):
        return (2, "send_email", {"user_id": 102}, 1, None)

    def fake_fail(**kwargs):
        failed_jobs.append(kwargs)
        return True

    def fake_complete(**kwargs):
        completed_jobs.append(kwargs)
        return True

    def handler(payload):
        raise RuntimeError("Email service unavailable")

    monkeypatch.setattr("app.worker.claim", fake_claim)
    monkeypatch.setattr("app.worker.fail", fake_fail)
    monkeypatch.setattr("app.worker.complete", fake_complete)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": handler},
    )

    result = worker.run_once()

    assert result is True
    assert len(failed_jobs) == 1
    assert failed_jobs[0]["job_id"] == 2
    assert failed_jobs[0]["error"] == (
        "Handler raised RuntimeError; message and traceback omitted."
    )
    assert completed_jobs == []


def test_worker_does_not_log_or_store_handler_exception_details(
    monkeypatch, caplog
):
    secret_payload_value = "customer-private-value-409"
    secret_token = "api-token-super-secret-982"
    failure_calls = []

    monkeypatch.setattr(
        "app.worker.claim",
        lambda **kwargs: (
            22,
            "send_email",
            {"customer": secret_payload_value},
            1,
            None,
        ),
    )
    monkeypatch.setattr(
        "app.worker.fail",
        lambda **kwargs: failure_calls.append(kwargs) or True,
    )

    def handler(payload):
        raise RuntimeError(
            f"request failed for {payload['customer']} with token={secret_token}"
        )

    worker = Worker("worker-safe-errors", {"send_email": handler})
    assert worker.run_once() is True

    stored_summary = failure_calls[0]["error"]
    assert stored_summary == (
        "Handler raised RuntimeError; message and traceback omitted."
    )
    assert secret_payload_value not in stored_summary
    assert secret_token not in stored_summary
    assert secret_payload_value not in caplog.text
    assert secret_token not in caplog.text
    assert "Traceback" not in caplog.text


@pytest.mark.parametrize("rejected_operation", ["complete", "fail"])
def test_worker_logs_rejected_update_without_retrying_state_change(
    monkeypatch, caplog, rejected_operation
):
    completion_calls = []
    failure_calls = []

    monkeypatch.setattr(
        "app.worker.claim",
        lambda **kwargs: (4, "send_email", {"user_id": 104}, 1, None),
    )

    def fake_complete(**kwargs):
        completion_calls.append(kwargs)
        return False

    def fake_fail(**kwargs):
        failure_calls.append(kwargs)
        return False

    def handler(payload):
        if rejected_operation == "fail":
            raise RuntimeError("Handler failed")

    monkeypatch.setattr("app.worker.complete", fake_complete)
    monkeypatch.setattr("app.worker.fail", fake_fail)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": handler},
    )

    assert worker.run_once() is True

    if rejected_operation == "complete":
        assert len(completion_calls) == 1
        assert failure_calls == []
    else:
        assert completion_calls == []
        assert len(failure_calls) == 1

    assert "rejected by lease or fencing checks" in caplog.text


def test_worker_fails_job_when_handler_is_missing(monkeypatch):
    failed_jobs = []

    def fake_claim(**kwargs):
        return (3, "unknown_job", {"user_id": 103}, 1, None)

    def fake_fail(**kwargs):
        failed_jobs.append(kwargs)
        return True

    monkeypatch.setattr("app.worker.claim", fake_claim)
    monkeypatch.setattr("app.worker.fail", fake_fail)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": lambda payload: None},
    )

    result = worker.run_once()

    assert result is True
    assert len(failed_jobs) == 1
    assert failed_jobs[0]["job_id"] == 3
    assert failed_jobs[0]["error"].startswith("Handler raised ValueError")


def test_worker_stop_sets_stop_event():
    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": lambda payload: None},
    )

    assert worker.stop_event.is_set() is False

    worker.stop()

    assert worker.stop_event.is_set() is True


def test_worker_executes_real_database_job():
    executed_payloads = []

    job = enqueue(
        job_type="send_email",
        payload={"user_id": 201},
        idempotency_key=f"worker-test-{uuid4()}",
    )

    def send_email(payload):
        executed_payloads.append(payload)

    worker = Worker(
        worker_id="integration-worker",
        handlers={"send_email": send_email},
    )

    result = worker.run_once()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts FROM jobs WHERE id = %s",
            (job[0],),
        )
        status, attempts = cur.fetchone()

    assert result is True
    assert executed_payloads == [{"user_id": 201}]
    assert status == "completed"
    assert attempts == 1


def test_worker_failure_updates_real_database():
    job = enqueue(
        job_type="send_email",
        payload={"user_id": 202},
        idempotency_key=f"worker-failure-{uuid4()}",
    )

    def send_email(payload):
        raise RuntimeError("Email service unavailable")

    worker = Worker(
        worker_id="integration-worker",
        handlers={"send_email": send_email},
    )

    result = worker.run_once()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT status, attempts, last_error, worker_id
                FROM jobs
                WHERE id = %s
                """,
            (job[0],),
        )
        status, attempts, last_error, worker_id = cur.fetchone()

    assert result is True
    assert status == "queued"
    assert attempts == 1
    assert last_error == (
        "Handler raised RuntimeError; message and traceback omitted."
    )
    assert worker_id is None


def test_worker_run_forever_stops_gracefully(monkeypatch):
    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": lambda payload: None},
        poll_interval_seconds=0.1,
    )

    run_once_called = threading.Event()

    def fake_run_once():
        run_once_called.set()
        return False

    monkeypatch.setattr(worker, "run_once", fake_run_once)

    thread = threading.Thread(target=worker.run_forever)
    thread.start()

    assert run_once_called.wait(timeout=2)

    worker.stop()
    thread.join(timeout=2)

    assert not thread.is_alive()


def test_worker_renews_heartbeat_during_long_running_job(monkeypatch):
    heartbeat_called = threading.Event()
    heartbeat_calls = []

    def fake_claim(**kwargs):
        return (10, "send_email", {"user_id": 110}, 1, None)

    def fake_heartbeat(**kwargs):
        heartbeat_calls.append(kwargs)
        heartbeat_called.set()
        return True

    def fake_complete(**kwargs):
        return True

    def handler(payload):
        assert heartbeat_called.wait(timeout=3)

    monkeypatch.setattr("app.worker.claim", fake_claim)
    monkeypatch.setattr("app.worker.heartbeat", fake_heartbeat)
    monkeypatch.setattr("app.worker.complete", fake_complete)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": handler},
        visibility_timeout_seconds=1,
    )

    result = worker.run_once()

    assert result is True
    assert len(heartbeat_calls) >= 1
    assert heartbeat_calls[0]["job_id"] == 10
    assert heartbeat_calls[0]["attempt"] == 1


def test_worker_handles_lost_heartbeat_lease(monkeypatch):
    heartbeat_called = threading.Event()
    cancellation_observed = threading.Event()
    cancellation_events = []
    completion_calls = []
    failure_calls = []

    def fake_claim(**kwargs):
        return (11, "send_email", {"user_id": 111}, 1, None)

    def fake_heartbeat(**kwargs):
        heartbeat_called.set()
        return False

    def fake_complete(**kwargs):
        completion_calls.append(kwargs)

    def fake_fail(**kwargs):
        failure_calls.append(kwargs)

    def handler(payload, cancel_event):
        cancellation_events.append(cancel_event)
        assert heartbeat_called.wait(timeout=3)
        assert cancel_event.wait(timeout=3)
        cancellation_observed.set()

    monkeypatch.setattr("app.worker.claim", fake_claim)
    monkeypatch.setattr("app.worker.heartbeat", fake_heartbeat)
    monkeypatch.setattr("app.worker.complete", fake_complete)
    monkeypatch.setattr("app.worker.fail", fake_fail)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": CancellableHandler(handler)},
        visibility_timeout_seconds=1,
    )

    result = worker.run_once()

    assert result is True
    assert cancellation_observed.is_set()
    assert len(cancellation_events) == 1
    assert cancellation_events[0].is_set()
    assert completion_calls == []
    assert failure_calls == []


def test_worker_cancels_handler_and_skips_updates_when_heartbeat_raises(
    monkeypatch, caplog
):
    heartbeat_called = threading.Event()
    cancellation_observed = threading.Event()
    completion_calls = []
    failure_calls = []

    def fake_claim(**kwargs):
        return (12, "send_email", {"user_id": 112}, 1, None)

    def fake_heartbeat(**kwargs):
        heartbeat_called.set()
        raise TimeoutError("database timeout containing sensitive-token")

    def fake_complete(**kwargs):
        completion_calls.append(kwargs)

    def fake_fail(**kwargs):
        failure_calls.append(kwargs)

    def handler(payload, cancel_event):
        assert heartbeat_called.wait(timeout=3)
        assert cancel_event.wait(timeout=3)
        cancellation_observed.set()
        raise RuntimeError("Handler stopped after lease loss")

    monkeypatch.setattr("app.worker.claim", fake_claim)
    monkeypatch.setattr("app.worker.heartbeat", fake_heartbeat)
    monkeypatch.setattr("app.worker.complete", fake_complete)
    monkeypatch.setattr("app.worker.fail", fake_fail)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": CancellableHandler(handler)},
        visibility_timeout_seconds=1,
    )

    result = worker.run_once()

    assert result is True
    assert cancellation_observed.is_set()
    assert completion_calls == []
    assert failure_calls == []
    assert "sensitive-token" not in caplog.text
    assert "TimeoutError" in caplog.text
    assert "Traceback" not in caplog.text


def test_worker_does_not_wait_indefinitely_for_heartbeat_thread(
    monkeypatch, caplog
):
    heartbeat_started = threading.Event()
    release_heartbeat = threading.Event()
    heartbeat_finished = threading.Event()
    completion_calls = []

    monkeypatch.setattr(
        "app.worker.claim",
        lambda **kwargs: (13, "send_email", {}, 1, None),
    )
    monkeypatch.setattr(
        "app.worker.HEARTBEAT_THREAD_JOIN_TIMEOUT_SECONDS",
        0.01,
    )

    def fake_heartbeat(**kwargs):
        heartbeat_started.set()
        release_heartbeat.wait()
        heartbeat_finished.set()
        return True

    monkeypatch.setattr("app.worker.heartbeat", fake_heartbeat)
    monkeypatch.setattr(
        "app.worker.complete",
        lambda **kwargs: completion_calls.append(kwargs) or True,
    )
    worker = Worker(
        "worker-bounded-heartbeat",
        {"send_email": lambda payload: heartbeat_started.wait(timeout=2)},
        visibility_timeout_seconds=1,
    )

    try:
        assert worker.run_once() is True
        assert completion_calls
        assert "did not stop before the join deadline" in caplog.text
        assert not heartbeat_finished.is_set()
    finally:
        release_heartbeat.set()

    assert heartbeat_finished.wait(timeout=2)


def test_worker_finishes_active_job_before_stopping(monkeypatch):
    handler_started = threading.Event()
    release_handler = threading.Event()
    cancellation_events = []
    completed_jobs = []

    def fake_claim(**kwargs):
        return (20, "send_email", {"user_id": 120}, 1, None)

    def fake_complete(**kwargs):
        completed_jobs.append(kwargs)
        return True

    def handler(payload, cancel_event):
        cancellation_events.append(cancel_event)
        handler_started.set()
        assert release_handler.wait(timeout=3)

    monkeypatch.setattr("app.worker.claim", fake_claim)
    monkeypatch.setattr("app.worker.complete", fake_complete)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": CancellableHandler(handler)},
    )

    thread = threading.Thread(target=worker.run_forever)
    thread.start()

    assert handler_started.wait(timeout=2)

    worker.stop()

    assert thread.is_alive()
    assert cancellation_events[0].is_set() is False

    release_handler.set()
    thread.join(timeout=3)

    assert not thread.is_alive()
    assert cancellation_events[0].is_set() is False
    assert len(completed_jobs) == 1
    assert completed_jobs[0]["job_id"] == 20


def test_worker_does_not_claim_new_job_after_stop(monkeypatch):
    claim_calls = []

    def fake_claim(**kwargs):
        claim_calls.append(kwargs)
        return None

    monkeypatch.setattr("app.worker.claim", fake_claim)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": lambda payload: None},
    )

    worker.stop()

    result = worker.run_once()

    assert result is False
    assert len(claim_calls) == 0


def test_worker_does_not_claim_second_job_after_active_job_finishes(monkeypatch):
    claim_calls = []
    handler_started = threading.Event()
    release_handler = threading.Event()
    completed_jobs = []

    def fake_claim(**kwargs):
        claim_calls.append(kwargs)
        return (21, "send_email", {"user_id": 121}, 1, None)

    def fake_complete(**kwargs):
        completed_jobs.append(kwargs)
        return True

    def handler(payload):
        handler_started.set()
        assert release_handler.wait(timeout=3)

    monkeypatch.setattr("app.worker.claim", fake_claim)
    monkeypatch.setattr("app.worker.complete", fake_complete)

    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": handler},
    )

    thread = threading.Thread(target=worker.run_forever)
    thread.start()

    assert handler_started.wait(timeout=2)

    worker.stop()
    release_handler.set()

    thread.join(timeout=3)

    assert not thread.is_alive()
    assert len(claim_calls) == 1
    assert len(completed_jobs) == 1


def test_worker_graceful_shutdown_completes_real_database_job():
    handler_started = threading.Event()
    release_handler = threading.Event()
    executed_payloads = []

    job = enqueue(
        job_type="send_email",
        payload={"user_id": 301},
        idempotency_key=f"graceful-shutdown-{uuid4()}",
    )

    def send_email(payload):
        executed_payloads.append(payload)
        handler_started.set()
        assert release_handler.wait(timeout=10)

    worker = Worker(
        worker_id="integration-worker",
        handlers={"send_email": send_email},
        poll_interval_seconds=0.1,
    )

    worker_thread = threading.Thread(target=worker.run_forever)
    worker_thread.start()

    try:
        assert handler_started.wait(timeout=5)

        worker.stop()
        release_handler.set()

        worker_thread.join(timeout=5)

        assert not worker_thread.is_alive()

    finally:
        release_handler.set()
        worker.stop()
        worker_thread.join(timeout=5)

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, attempts, worker_id
            FROM jobs
            WHERE id = %s
            """,
            (job[0],),
        )
        status, attempts, worker_id = cur.fetchone()

    assert executed_payloads == [{"user_id": 301}]
    assert status == "completed"
    assert attempts == 1
    assert worker_id is None


def test_run_worker_with_signals_requires_main_thread():
    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": lambda payload: None},
    )

    errors = []

    def run():
        try:
            run_worker_with_signals(worker)
        except RuntimeError as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert "main thread" in str(errors[0])


def test_run_worker_with_signals_restores_previous_handlers(monkeypatch):
    worker = Worker(
        worker_id="worker-1",
        handlers={"send_email": lambda payload: None},
    )

    previous_handlers = {}

    for sig in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[sig] = signal.getsignal(sig)

    monkeypatch.setattr(
        worker,
        "run_forever",
        lambda: None,
    )

    run_worker_with_signals(worker)

    for sig, previous_handler in previous_handlers.items():
        assert signal.getsignal(sig) == previous_handler


def test_worker_handles_sigterm_and_completes_active_job():
    job = enqueue(
        job_type="send_email",
        payload={"user_id": 401},
        idempotency_key=f"sigterm-test-{uuid4()}",
    )

    child_code = """
import signal
import threading
import time

from app.worker import Worker, run_worker_with_signals

handler_started = threading.Event()

def handler(payload):
    print("HANDLER_STARTED", flush=True)
    handler_started.set()

    time.sleep(1)

worker = Worker(
    worker_id="sigterm-worker",
    handlers={"send_email": handler},
    poll_interval_seconds=0.1,
    visibility_timeout_seconds=5,
)

run_worker_with_signals(worker)
"""

    process = subprocess.Popen(
        [sys.executable, "-c", child_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )

    try:
        assert _wait_for_stdout_marker(
            process,
            "HANDLER_STARTED",
            timeout=5,
        ), "Worker handler did not start"

        process.send_signal(signal.SIGTERM)

        stdout, stderr = process.communicate(timeout=10)

        assert process.returncode == 0, stderr

    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts FROM jobs WHERE id = %s",
            (job[0],),
        )
        status, attempts = cur.fetchone()

    assert status == "completed"
    assert attempts == 1


def test_worker_handles_sigint_and_exits_cleanly():
    child_code = """
from app.worker import Worker, run_worker_with_signals

class ReadyWorker(Worker):
    def run_forever(self):
        # run_worker_with_signals installs handlers before this method runs.
        print("WORKER_SIGNALS_READY", flush=True)
        super().run_forever()

worker = ReadyWorker(
    worker_id="sigint-worker",
    handlers={"send_email": lambda payload: None},
    poll_interval_seconds=0.1,
)

run_worker_with_signals(worker)
"""

    process = subprocess.Popen(
        [sys.executable, "-c", child_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )

    try:
        assert _wait_for_stdout_marker(
            process,
            "WORKER_SIGNALS_READY",
            timeout=5,
        ), "Worker signal handlers were not installed before readiness"

        process.send_signal(signal.SIGINT)

        stdout, stderr = process.communicate(timeout=10)

        assert process.returncode == 0, stderr

    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
