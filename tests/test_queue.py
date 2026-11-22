import psycopg
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

import app.queue as queue_module
from app.database import get_connection
from app.queue import claim, complete, enqueue, fail, heartbeat


@pytest.fixture
def idempotency_key():
    return f"test-{uuid4()}"


@pytest.fixture(autouse=True)
def clean_jobs_table():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")

    yield

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")


def _expire_job(job_id):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET visible_at = clock_timestamp() - INTERVAL '1 second'
            WHERE id = %s
            RETURNING visible_at
            """,
            (job_id,),
        )
        return cur.fetchone()[0]


def _lease_job_state(job_id):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, attempts, worker_id, last_error, completed_at, visible_at
            FROM jobs
            WHERE id = %s
            """,
            (job_id,),
        )
        return cur.fetchone()


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

    with get_connection() as conn, conn.cursor() as cur:
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
    run_at = datetime.now(UTC) + timedelta(hours=1)

    job = enqueue(
        "send_email",
        {"user_id": 101},
        idempotency_key,
        run_at=run_at,
    )

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT visible_at FROM jobs WHERE id = %s",
            (job[0],),
        )
        visible_at = cur.fetchone()[0]

    assert abs((visible_at - run_at).total_seconds()) < 0.001


@pytest.mark.parametrize("payload", [None, "invalid", 123, []])
def test_invalid_payload_rejected(payload):
    with pytest.raises(TypeError):
        enqueue("test_job", payload)


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

    with get_connection() as conn, conn.cursor() as cur:
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
    assert visible_at > datetime.now(UTC)


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

    with get_connection() as conn, conn.cursor() as cur:
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

    with get_connection() as conn, conn.cursor() as cur:
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

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts FROM jobs WHERE id = %s",
            (first_claim[0],),
        )
        result = cur.fetchone()

    assert result == ("dead", 1)


def test_claim_skips_locked_exhausted_job_and_claims_eligible_job(
    monkeypatch,
):
    exhausted_job = enqueue("send_email", {"user_id": 104}, max_attempts=1)
    eligible_job = enqueue("send_email", {"user_id": 105})

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET attempts = max_attempts,
                visible_at = clock_timestamp() - INTERVAL '1 second'
            WHERE id = %s
            """,
            (exhausted_job[0],),
        )

    original_get_connection = queue_module.get_connection
    claim_connection_ready = threading.Event()

    def tracked_connection():
        conn = original_get_connection()
        claim_connection_ready.set()
        return conn

    monkeypatch.setattr(queue_module, "get_connection", tracked_connection)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with get_connection() as lock_conn:
            with lock_conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM jobs WHERE id = %s FOR UPDATE",
                    (exhausted_job[0],),
                )
                assert cur.fetchone() == (exhausted_job[0],)

            claim_future = executor.submit(claim, "available-worker")
            assert claim_connection_ready.wait(timeout=3)

            try:
                claimed_job = claim_future.result(timeout=5)
                completed_before_unlock = True
            except FutureTimeout:
                claimed_job = None
                completed_before_unlock = False

        # Release the row lock before waiting again, so a regression fails
        # cleanly instead of leaving a blocked worker thread behind.
        if not completed_before_unlock:
            claim_future.result(timeout=5)

    assert completed_before_unlock
    assert claimed_job[0] == eligible_job[0]

    assert claim("cleanup-worker") is None
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM jobs WHERE id = %s",
            (exhausted_job[0],),
        )
        assert cur.fetchone() == ("dead",)


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
        job_id for worker_results in results for job_id in worker_results
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
    assert job is not None

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE jobs SET visible_at = clock_timestamp() + INTERVAL '1 hour' WHERE id = %s",
            (job[0],),
        )

    result = complete(
        job_id=job[0],
        worker_id="worker-1",
        attempt=job[3],
    )

    assert result is True

    with get_connection() as conn, conn.cursor() as cur:
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


def test_complete_rejects_expired_lease_without_reclaim(idempotency_key):
    enqueue("send_email", {"user_id": 106}, idempotency_key)
    job = claim("worker-1")
    assert job is not None

    expired_at = _expire_job(job[0])

    assert complete(job[0], "worker-1", job[3]) is False
    assert _lease_job_state(job[0]) == (
        "running",
        job[3],
        "worker-1",
        None,
        None,
        expired_at,
    )


def test_stale_worker_cannot_complete_job(idempotency_key):
    enqueue(
        "send_email",
        {"user_id": 106},
        idempotency_key,
    )

    first_claim = claim("worker-1")

    with get_connection() as conn, conn.cursor() as cur:
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
    assert (
        complete(
            job_id=second_claim[0],
            worker_id="worker-2",
            attempt=second_claim[3],
        )
        is True
    )


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

    with get_connection() as conn, conn.cursor() as cur:
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


def test_retry_delay_progression_stops_at_maximum_cap(
    idempotency_key, monkeypatch
):
    job = enqueue(
        "send_email",
        {"user_id": 107},
        idempotency_key,
        max_attempts=5,
    )
    observed_bounds = []

    def choose_midpoint(lower, upper):
        observed_bounds.append((lower, upper))
        return (lower + upper) / 2

    monkeypatch.setattr(queue_module.random, "uniform", choose_midpoint)
    expected_exponential_delays = [2, 4, 8, 10, 10]

    for attempt_number, expected_delay in enumerate(
        expected_exponential_delays,
        start=1,
    ):
        claimed = claim(f"retry-worker-{attempt_number}")
        assert claimed is not None
        assert claimed[0] == job[0]
        assert claimed[3] == attempt_number

        assert fail(
            claimed[0],
            f"retry-worker-{attempt_number}",
            claimed[3],
            "temporary failure",
            base_delay_seconds=2,
            max_delay_seconds=10,
        ) is True

        assert observed_bounds[-1] == (
            expected_delay / 2,
            expected_delay,
        )

        if attempt_number < len(expected_exponential_delays):
            with get_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET visible_at = clock_timestamp() - INTERVAL '1 second'
                    WHERE id = %s
                    """,
                    (job[0],),
                )

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT status, attempts FROM jobs WHERE id = %s", (job[0],))
        assert cur.fetchone() == ("dead", 5)


@pytest.mark.parametrize("jitter_edge", ["lower", "upper"])
def test_retry_jitter_persists_within_exponential_bounds(
    idempotency_key, monkeypatch, jitter_edge
):
    job = enqueue("send_email", {"user_id": 108}, idempotency_key)
    claimed = claim("jitter-worker")
    assert claimed is not None
    bounds = []

    def choose_edge(lower, upper):
        bounds.append((lower, upper))
        return lower if jitter_edge == "lower" else upper

    monkeypatch.setattr(queue_module.random, "uniform", choose_edge)

    assert fail(
        claimed[0],
        "jitter-worker",
        claimed[3],
        "temporary failure",
        base_delay_seconds=4,
        max_delay_seconds=20,
    ) is True

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT visible_at - clock_timestamp() FROM jobs WHERE id = %s",
            (job[0],),
        )
        persisted_delay = cur.fetchone()[0].total_seconds()

    assert bounds == [(2, 4)]
    expected_delay = 2 if jitter_edge == "lower" else 4
    assert abs(persisted_delay - expected_delay) < 0.5


def test_fail_redacts_credentials_and_bounds_last_error(idempotency_key):
    enqueue("send_email", {"user_id": 107}, idempotency_key)
    job = claim("worker-1")
    assert job is not None

    secret_password = "db-pass-value-882"
    secret_token = "bearer-token-value-229"
    quoted_secret = "multi word private token 443"
    secret_jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signaturelongvalue"
    raw_error = (
        f"password={secret_password}; "
        f'"refresh_token": "{quoted_secret}"; Bearer {secret_token}; '
        f"postgresql://queue:{secret_password}@db/durableq; {secret_jwt}; "
        + "x" * 400
    )

    assert fail(job[0], "worker-1", job[3], raw_error) is True

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT last_error FROM jobs WHERE id = %s", (job[0],))
        stored_error = cur.fetchone()[0]

    assert len(stored_error) <= queue_module.MAX_LAST_ERROR_LENGTH
    assert secret_password not in stored_error
    assert secret_token not in stored_error
    assert quoted_secret not in stored_error
    assert secret_jwt not in stored_error
    assert "[REDACTED]" in stored_error


def test_fail_rejects_expired_lease_without_reclaim(idempotency_key):
    enqueue("send_email", {"user_id": 107}, idempotency_key)
    job = claim("worker-1")
    assert job is not None

    expired_at = _expire_job(job[0])

    assert (
        fail(
            job[0],
            "worker-1",
            job[3],
            "late failure",
        )
        is False
    )
    assert _lease_job_state(job[0]) == (
        "running",
        job[3],
        "worker-1",
        None,
        None,
        expired_at,
    )


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

    with get_connection() as conn, conn.cursor() as cur:
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

    with get_connection() as conn, conn.cursor() as cur:
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
    assert (
        fail(
            job_id=second_claim[0],
            worker_id="worker-2",
            attempt=second_claim[3],
            error="Current worker failure",
        )
        is True
    )


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

    with get_connection() as conn, conn.cursor() as cur:
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


def test_concurrent_workers_reclaim_expired_job_once(idempotency_key):
    enqueue(
        job_type="send_email",
        payload={"user_id": 301},
        idempotency_key=idempotency_key,
    )

    first_claim = claim(
        worker_id="original-worker",
        visibility_timeout_seconds=60,
    )
    assert first_claim is not None

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
            (first_claim[0],),
        )

    def reclaim(worker_number):
        return claim(worker_id=f"recovery-worker-{worker_number}")

    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(reclaim, range(20)))

    successful_claims = [job for job in results if job is not None]

    assert len(successful_claims) == 1
    assert successful_claims[0][0] == first_claim[0]
    assert successful_claims[0][3] == 2


def test_crashed_worker_job_is_recovered(idempotency_key):
    enqueue(
        job_type="send_email",
        payload={"user_id": 302},
        idempotency_key=idempotency_key,
    )

    crashed_job = claim(
        worker_id="worker-1",
        visibility_timeout_seconds=60,
    )

    # Simulate worker crash: no complete() or fail() call.

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
            (crashed_job[0],),
        )

    recovered_job = claim(worker_id="worker-2")

    assert recovered_job is not None
    assert recovered_job[0] == crashed_job[0]
    assert recovered_job[3] == 2


def test_job_recovers_after_actual_worker_process_crash(idempotency_key):
    job = enqueue(
        job_type="send_email",
        payload={"user_id": 303},
        idempotency_key=idempotency_key,
    )

    job_id = job[0]

    child_code = """
import os
from app.queue import claim

job = claim(
    worker_id="crashed-process",
    visibility_timeout_seconds=60,
)

if job is None:
    os._exit(2)

print(job[0], flush=True)
os._exit(1)
"""

    result = subprocess.run(
        [sys.executable, "-c", child_code],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        timeout=10,
        check=False,
    )

    assert result.returncode == 1
    assert result.stdout.strip() == str(job_id)

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
            (job_id,),
        )

    recovered_job = claim(worker_id="recovery-worker")

    assert recovered_job is not None
    assert recovered_job[0] == job_id
    assert recovered_job[3] == 2


def test_multiple_expired_jobs_can_be_recovered():
    job_ids = []

    for i in range(5):
        job = enqueue(
            job_type="send_email",
            payload={"user_id": 400 + i},
        )
        job_ids.append(job[0])

        claimed = claim(
            worker_id=f"worker-{i}",
            visibility_timeout_seconds=60,
        )

        assert claimed[0] == job[0]

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = ANY(%s)
                """,
            (job_ids,),
        )

    recovered_ids = []

    for i in range(5):
        job = claim(worker_id=f"recovery-worker-{i}")
        assert job is not None
        recovered_ids.append(job[0])

    assert set(recovered_ids) == set(job_ids)
    assert len(recovered_ids) == len(set(recovered_ids))


def test_expired_job_recovery_preserves_attempt_limit():
    enqueue(
        "send_email",
        {"user_id": 501},
        max_attempts=2,
    )

    first = claim("worker-1")
    assert first[3] == 1

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
            (first[0],),
        )

    second = claim("worker-2")
    assert second is not None
    assert second[3] == 2

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
            (first[0],),
        )

    third = claim("worker-3")
    assert third is None

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts FROM jobs WHERE id = %s",
            (first[0],),
        )
        assert cur.fetchone() == ("dead", 2)


def test_completed_job_cannot_be_reclaimed():
    enqueue(
        "send_email",
        {"user_id": 502},
    )

    first = claim("worker-1")

    assert (
        complete(
            first[0],
            "worker-1",
            first[3],
        )
        is True
    )

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
                UPDATE jobs
                SET visible_at = NOW() - INTERVAL '1 second'
                WHERE id = %s
                """,
            (first[0],),
        )

    second = claim("worker-2")

    assert second is None


def test_failed_job_is_not_claimed_before_retry_time():
    enqueue(
        "send_email",
        {"user_id": 503},
    )

    first = claim("worker-1")

    fail(
        first[0],
        "worker-1",
        first[3],
        "Temporary error",
        base_delay_seconds=10,
        max_delay_seconds=10,
    )

    second = claim("worker-2")

    assert second is None

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, attempts FROM jobs WHERE id = %s",
            (first[0],),
        )
        assert cur.fetchone() == ("queued", 1)


def test_claim_propagates_database_connection_error(monkeypatch):
    def raise_connection_error():
        raise RuntimeError("Database unavailable")

    monkeypatch.setattr(
        queue_module,
        "get_connection",
        raise_connection_error,
    )

    with pytest.raises(RuntimeError, match="Database unavailable"):
        claim("worker-1")


def test_complete_propagates_database_connection_error(monkeypatch):
    def raise_connection_error():
        raise RuntimeError("Database unavailable")

    monkeypatch.setattr(
        queue_module,
        "get_connection",
        raise_connection_error,
    )

    with pytest.raises(RuntimeError, match="Database unavailable"):
        complete(
            job_id=1,
            worker_id="worker-1",
            attempt=1,
        )


def test_fail_propagates_database_connection_error(monkeypatch):
    def raise_connection_error():
        raise RuntimeError("Database unavailable")

    monkeypatch.setattr(
        queue_module,
        "get_connection",
        raise_connection_error,
    )

    with pytest.raises(RuntimeError, match="Database unavailable"):
        fail(
            job_id=1,
            worker_id="worker-1",
            attempt=1,
            error="Temporary failure",
        )


def test_heartbeat_extends_job_lease(idempotency_key):
    enqueue("send_email", {"user_id": 601}, idempotency_key)
    job = claim("worker-1", visibility_timeout_seconds=10)

    assert job is not None

    assert heartbeat(job[0], "worker-1", job[3], 60) is True

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT visible_at > NOW() + INTERVAL '50 seconds' FROM jobs WHERE id = %s",
            (job[0],),
        )
        assert cur.fetchone()[0] is True


def test_stale_worker_cannot_extend_lease(idempotency_key):
    enqueue("send_email", {"user_id": 602}, idempotency_key)
    first = claim("worker-1", visibility_timeout_seconds=60)

    assert first is not None

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE jobs SET visible_at = NOW() - INTERVAL '1 second' WHERE id = %s",
            (first[0],),
        )

    second = claim("worker-2", visibility_timeout_seconds=60)

    assert second is not None

    assert heartbeat(first[0], "worker-1", first[3], 60) is False


def test_heartbeat_rejects_expired_lease_without_reclaim(idempotency_key):
    enqueue("send_email", {"user_id": 605}, idempotency_key)
    job = claim("worker-1", visibility_timeout_seconds=60)
    assert job is not None

    expired_at = _expire_job(job[0])

    assert heartbeat(job[0], "worker-1", job[3], 60) is False
    assert _lease_job_state(job[0]) == (
        "running",
        job[3],
        "worker-1",
        None,
        None,
        expired_at,
    )


@pytest.mark.parametrize("operation", ["heartbeat", "complete", "fail"])
def test_lease_operation_rechecks_expiry_after_row_lock_wait(
    idempotency_key, monkeypatch, operation
):
    enqueue("send_email", {"user_id": 606}, idempotency_key)
    job = claim("worker-1", visibility_timeout_seconds=60)
    assert job is not None

    real_queue_connection = queue_module.get_connection
    application_name = f"durableq-lease-lock-{uuid4().hex}"
    waiter_connected = threading.Event()
    waiter_pid = []

    def tracked_queue_connection():
        conn = real_queue_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT set_config('application_name', %s, false)",
                (application_name,),
            )
            cur.execute("SELECT pg_backend_pid()")
            waiter_pid.append(cur.fetchone()[0])
        waiter_connected.set()
        return conn

    monkeypatch.setattr(queue_module, "get_connection", tracked_queue_connection)

    def perform_operation():
        if operation == "heartbeat":
            return heartbeat(job[0], "worker-1", job[3], 60)
        if operation == "complete":
            return complete(job[0], "worker-1", job[3])
        return fail(job[0], "worker-1", job[3], "late failure")

    with ThreadPoolExecutor(max_workers=1) as executor:
        with get_connection() as lock_conn:
            with lock_conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM jobs WHERE id = %s FOR UPDATE",
                    (job[0],),
                )
                assert cur.fetchone() == (job[0],)

            operation_future = executor.submit(perform_operation)
            assert waiter_connected.wait(timeout=5)

            deadline = time.monotonic() + 5
            lock_wait_observed = False
            while time.monotonic() < deadline:
                if operation_future.done():
                    break

                with get_connection() as monitor_conn, monitor_conn.cursor() as cur:
                    cur.execute("SELECT pg_blocking_pids(%s)", (waiter_pid[0],))
                    if cur.fetchone()[0]:
                        lock_wait_observed = True
                        break

                threading.Event().wait(0.01)

            assert lock_wait_observed, "operation did not block on the held row lock"

            with lock_conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET visible_at = clock_timestamp() - INTERVAL '1 second'
                    WHERE id = %s
                    RETURNING visible_at
                    """,
                    (job[0],),
                )
                expired_at = cur.fetchone()[0]

        assert operation_future.result(timeout=5) is False

    assert _lease_job_state(job[0]) == (
        "running",
        job[3],
        "worker-1",
        None,
        None,
        expired_at,
    )


def test_wrong_attempt_cannot_extend_lease(idempotency_key):
    enqueue("send_email", {"user_id": 603}, idempotency_key)
    job = claim("worker-1")

    assert job is not None

    assert heartbeat(job[0], "worker-1", job[3] + 1, 60) is False


def test_completed_job_lease_cannot_be_extended(idempotency_key):
    enqueue("send_email", {"user_id": 604}, idempotency_key)
    job = claim("worker-1")

    assert job is not None

    assert complete(job[0], "worker-1", job[3]) is True

    assert heartbeat(job[0], "worker-1", job[3], 60) is False


@pytest.mark.parametrize(
    "kwargs",
    [
        {"job_id": 0, "worker_id": "worker-1", "attempt": 1},
        {"job_id": 1, "worker_id": "", "attempt": 1},
        {"job_id": 1, "worker_id": "worker-1", "attempt": 0},
        {
            "job_id": 1,
            "worker_id": "worker-1",
            "attempt": 1,
            "visibility_timeout_seconds": 0,
        },
    ],
)
def test_heartbeat_rejects_invalid_inputs(kwargs):
    with pytest.raises(ValueError):
        heartbeat(**kwargs)


def test_running_job_requires_worker_id():
    with get_connection() as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute("""
                INSERT INTO jobs (job_type, payload, status)
                VALUES ('send_email', '{"user_id": 1}', 'running')
            """)


def test_non_running_job_rejects_worker_id():
    with get_connection() as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute("""
                INSERT INTO jobs (job_type, payload, status, worker_id)
                VALUES ('send_email', '{"user_id": 1}', 'queued', 'worker-1')
            """)


def test_completed_job_requires_timestamp():
    with get_connection() as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute("""
                INSERT INTO jobs (job_type, payload, status)
                VALUES ('send_email', '{"user_id": 1}', 'completed')
            """)


def test_non_completed_job_rejects_completion_timestamp():
    with get_connection() as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute("""
                INSERT INTO jobs (
                    job_type, payload, status, completed_at
                )
                VALUES (
                    'send_email', '{"user_id": 1}', 'queued', NOW()
                )
            """)


def test_payload_must_be_json_object():
    with get_connection() as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute("""
                INSERT INTO jobs (job_type, payload)
                VALUES ('send_email', '[1, 2, 3]')
            """)
