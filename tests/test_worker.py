from app.worker import Worker
import pytest
import threading
from uuid import uuid4

from app.worker import Worker
from app.database import get_connection
from app.queue import enqueue
@pytest.fixture(autouse=True)
def clean_jobs_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")

    yield

    with get_connection() as conn:
        with conn.cursor() as cur:
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
    assert failed_jobs[0]["error"] == "Email service unavailable"
    assert completed_jobs == []

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
    assert "No handler registered" in failed_jobs[0]["error"]

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

    with get_connection() as conn:
        with conn.cursor() as cur:
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

    with get_connection() as conn:
        with conn.cursor() as cur:
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
    assert last_error == "Email service unavailable"
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