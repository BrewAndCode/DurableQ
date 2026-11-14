from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from app.database import get_connection
from app.queue import enqueue, claim, complete, fail


@pytest.fixture
def idempotency_key():
    return f"test-{uuid4()}"

@pytest.fixture(autouse=True)
def clean_jobs_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")

    yield

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")


def test_enqueue_creates_job(idempotency_key):
    job = enqueue(
        job_type="send_email",
        payload={"user_id": 101},
        idempotency_key=idempotency_key,
    )

    assert job[0] is not None
    assert job[1] == "send_email"
    assert job[2] == "queued"

def test_enqueue_stores_jsonb_payload(idempotency_key):
    payload = {"user_id": 101, "email": "test@example.com"}

    job = enqueue(
        job_type="send_email",
        payload=payload,
        idempotency_key=idempotency_key,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM jobs WHERE id = %s",
                (job[0],),
            )
            stored_payload = cur.fetchone()[0]

    assert stored_payload == payload


def test_duplicate_idempotency_key_returns_same_job(idempotency_key):
    first = enqueue(
        "send_email",
        {"user_id": 101},
        idempotency_key,
    )

    second = enqueue(
        "send_email",
        {"user_id": 101},
        idempotency_key,
    )

    assert first[0] == second[0]


def test_different_idempotency_keys_create_separate_jobs():
    first = enqueue(
        "send_email",
        {"user_id": 101},
        f"test-{uuid4()}",
    )

    second = enqueue(
        "send_email",
        {"user_id": 102},
        f"test-{uuid4()}",
    )

    assert first[0] != second[0]


def test_delayed_job_scheduling(idempotency_key):
    run_at = datetime.now(timezone.utc) + timedelta(hours=1)

    job = enqueue(
        "send_email",
        {"user_id": 101},
        idempotency_key,
        run_at=run_at,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT visible_at FROM jobs WHERE id = %s",
                (job[0],),
            )
            visible_at = cur.fetchone()[0]

    assert abs((visible_at - run_at).total_seconds()) < 0.001


@pytest.mark.parametrize("payload", [None, "invalid", 123, []])
def test_invalid_payload_rejected(payload):
    with pytest.raises(ValueError):
        enqueue("send_email", payload)


@pytest.mark.parametrize("max_attempts", [0, -1, True, 1.5])
def test_invalid_max_attempts_rejected(max_attempts):
    with pytest.raises(ValueError):
        enqueue("send_email", {}, max_attempts=max_attempts)


def test_naive_datetime_rejected():
    with pytest.raises(ValueError):
        enqueue(
            "send_email",
            {},
            run_at=datetime.now(),
        )

def test_claim_returns_available_job(idempotency_key):
    enqueue(
        job_type="send_email",
        payload={"user_id": 101},
        idempotency_key=idempotency_key,
    )

    job = claim(worker_id="worker-1")

    assert job is not None
    assert job[1] == "send_email"
    assert job[2] == {"user_id": 101}
    assert job[3] == 1

def test_claim_assigns_worker_and_visibility_timeout(idempotency_key):
    enqueue(
        job_type="send_email",
        payload={"user_id": 102},
        idempotency_key=idempotency_key,
    )

    job = claim(
        worker_id="worker-2",
        visibility_timeout_seconds=60,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, worker_id, visible_at
                FROM jobs
                WHERE id = %s
                """,
                (job[0],),
            )
            result = cur.fetchone()

    status, worker_id, visible_at = result

    assert status == "running"
    assert worker_id == "worker-2"
    assert visible_at > datetime.now(timezone.utc)

def test_claim_returns_none_when_queue_is_empty():
    job = claim(worker_id="worker-1")

    assert job is None

def test_expired_job_can_be_reclaimed(idempotency_key):
    enqueue(
        job_type="send_email",
        payload={"user_id": 103},
        idempotency_key=idempotency_key,
    )

    first_claim = claim(
        worker_id="worker-1",
        visibility_timeout_seconds=60,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
                (first_claim[0],),
            )

    second_claim = claim(worker_id="worker-2")

    assert second_claim is not None
    assert second_claim[0] == first_claim[0]
    assert second_claim[3] == 2

def test_exhausted_job_moves_to_dead(idempotency_key):
    enqueue(
        job_type="send_email",
        payload={"user_id": 104},
        idempotency_key=idempotency_key,
        max_attempts=1,
    )

    first_claim = claim(
        worker_id="worker-1",
        visibility_timeout_seconds=60,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
                (first_claim[0],),
            )

    second_claim = claim(worker_id="worker-2")

    assert second_claim is None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, attempts FROM jobs WHERE id = %s",
                (first_claim[0],),
            )
            result = cur.fetchone()

    assert result == ("dead", 1)

def test_concurrent_workers_claim_each_job_once():
    total_jobs = 100
    total_workers = 20

    for i in range(total_jobs):
        enqueue(
            job_type="send_email",
            payload={"user_id": i},
        )

    def worker(worker_number):
        claimed_ids = []

        while True:
            job = claim(
                worker_id=f"worker-{worker_number}",
                visibility_timeout_seconds=60,
            )

            if job is None:
                break

            claimed_ids.append(job[0])

        return claimed_ids

    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        results = list(executor.map(worker, range(total_workers)))

    all_claimed_ids = [
        job_id
        for worker_results in results
        for job_id in worker_results
    ]

    assert len(all_claimed_ids) == total_jobs
    assert len(set(all_claimed_ids)) == total_jobs

def test_complete_marks_job_as_completed(idempotency_key):
    enqueue(
        "send_email",
        {"user_id": 105},
        idempotency_key,
    )

    job = claim("worker-1")

    result = complete(
        job_id=job[0],
        worker_id="worker-1",
        attempt=job[3],
    )

    assert result is True

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, worker_id, completed_at
                FROM jobs
                WHERE id = %s
                """,
                (job[0],),
            )
            status, worker_id, completed_at = cur.fetchone()

    assert status == "completed"
    assert worker_id is None
    assert completed_at is not None


def test_stale_worker_cannot_complete_job(idempotency_key):
    enqueue(
        "send_email",
        {"user_id": 106},
        idempotency_key,
    )

    first_claim = claim("worker-1")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
                (first_claim[0],),
            )

    second_claim = claim("worker-2")

    result = complete(
        job_id=first_claim[0],
        worker_id="worker-1",
        attempt=first_claim[3],
    )

    assert second_claim[3] == 2
    assert result is False

def test_fail_schedules_retry(idempotency_key):
    enqueue(
        "send_email",
        {"user_id": 107},
        idempotency_key,
    )

    job = claim("worker-1")

    result = fail(
        job_id=job[0],
        worker_id="worker-1",
        attempt=job[3],
        error="Email service unavailable",
    )

    assert result is True

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, worker_id, last_error, visible_at > NOW()
                FROM jobs
                WHERE id = %s
                """,
                (job[0],),
            )
            status, worker_id, last_error, is_delayed = cur.fetchone()

    assert status == "queued"
    assert worker_id is None
    assert last_error == "Email service unavailable"
    assert is_delayed is True

def test_fail_moves_exhausted_job_to_dead(idempotency_key):
    enqueue(
        "send_email",
        {"user_id": 108},
        idempotency_key,
        max_attempts=1,
    )

    job = claim("worker-1")

    result = fail(
        job_id=job[0],
        worker_id="worker-1",
        attempt=job[3],
        error="Permanent failure",
    )

    assert result is True

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, attempts, last_error
                FROM jobs
                WHERE id = %s
                """,
                (job[0],),
            )
            result = cur.fetchone()

    assert result == ("dead", 1, "Permanent failure")   

def test_stale_worker_cannot_fail_job(idempotency_key):
    enqueue(
        "send_email",
        {"user_id": 109},
        idempotency_key,
    )

    first_claim = claim("worker-1")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
                (first_claim[0],),
            )

    second_claim = claim("worker-2")

    result = fail(
        job_id=first_claim[0],
        worker_id="worker-1",
        attempt=first_claim[3],
        error="Stale worker error",
    )

    assert second_claim[3] == 2
    assert result is False

def test_failed_job_can_be_claimed_again(idempotency_key):
    enqueue(
        "send_email",
        {"user_id": 110},
        idempotency_key,
    )

    first_claim = claim("worker-1")

    fail(
        job_id=first_claim[0],
        worker_id="worker-1",
        attempt=first_claim[3],
        error="Temporary failure",
        base_delay_seconds=1,
        max_delay_seconds=1,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
                (first_claim[0],),
            )

    second_claim = claim("worker-2")

    assert second_claim is not None
    assert second_claim[0] == first_claim[0]
    assert second_claim[3] == 2