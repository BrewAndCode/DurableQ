import os
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

import app.metrics as metrics_module
from app.database import get_connection
from app.migrate import run_migrations
from app.queue import claim, complete, enqueue, fail
from app.metrics import get_queue_metrics


@pytest.fixture
def isolated_database(monkeypatch):
    test_database_url = os.environ["TEST_DATABASE_URL"]
    database_name = f"durableq_test_metrics_{uuid4().hex}"

    with psycopg.connect(test_database_url, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))

    isolated_url = make_conninfo(test_database_url, dbname=database_name)

    try:
        monkeypatch.setenv("DATABASE_URL", isolated_url)
        run_migrations()
        yield
    finally:
        with psycopg.connect(test_database_url, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(database_name)
                )
            )


def _enqueue(max_attempts=3):
    return enqueue(
        job_type="metrics_test",
        payload={"test": True},
        idempotency_key=f"metrics-{uuid4()}",
        max_attempts=max_attempts,
    )


def test_empty_queue_returns_zero_counts_and_exact_metrics_contract(
    isolated_database,
):
    metrics = get_queue_metrics()

    assert set(metrics) == {
        "total_jobs",
        "queued_jobs",
        "running_jobs",
        "completed_jobs",
        "dead_jobs",
        "eligible_jobs",
        "oldest_queued_age_seconds",
    }
    assert metrics == {
        "total_jobs": 0,
        "queued_jobs": 0,
        "running_jobs": 0,
        "completed_jobs": 0,
        "dead_jobs": 0,
        "eligible_jobs": 0,
        "oldest_queued_age_seconds": None,
    }
    assert all(
        type(metrics[name]) is int
        for name in (
            "total_jobs",
            "queued_jobs",
            "running_jobs",
            "completed_jobs",
            "dead_jobs",
            "eligible_jobs",
        )
    )


def test_metrics_count_mixed_states_and_keep_expired_running_jobs_running(
    isolated_database,
):
    running_job = _enqueue()
    running_claim = claim("worker-running")
    assert running_claim[0] == running_job[0]

    completed_job = _enqueue()
    completed_claim = claim("worker-completed")
    assert completed_claim[0] == completed_job[0]
    assert complete(completed_job[0], "worker-completed", completed_claim[3])

    dead_job = _enqueue(max_attempts=1)
    dead_claim = claim("worker-dead")
    assert dead_claim[0] == dead_job[0]
    assert fail(
        dead_job[0],
        "worker-dead",
        dead_claim[3],
        "permanent failure",
    )

    queued_job = _enqueue()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET visible_at = statement_timestamp() - INTERVAL '1 second'
            WHERE id = %s
            """,
            (running_job[0],),
        )

    metrics = get_queue_metrics()

    assert metrics["total_jobs"] == 4
    assert metrics["queued_jobs"] == 1
    assert metrics["running_jobs"] == 1
    assert metrics["completed_jobs"] == 1
    assert metrics["dead_jobs"] == 1
    assert metrics["eligible_jobs"] == 1
    assert queued_job[0] != running_job[0]


def test_delayed_queued_job_is_not_eligible(isolated_database):
    ready_job = _enqueue()
    delayed_job = _enqueue()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET visible_at = statement_timestamp() + INTERVAL '1 hour'
            WHERE id = %s
            """,
            (delayed_job[0],),
        )

    metrics = get_queue_metrics()

    assert metrics["total_jobs"] == 2
    assert metrics["queued_jobs"] == 2
    assert metrics["eligible_jobs"] == 1
    assert ready_job[0] != delayed_job[0]


def test_oldest_queued_age_uses_oldest_queued_created_at(isolated_database):
    completed_job = _enqueue()
    completed_claim = claim("worker-completed")
    assert complete(completed_job[0], "worker-completed", completed_claim[3])

    older_queued_job = _enqueue()
    newer_queued_job = _enqueue()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET created_at = statement_timestamp() - INTERVAL '5 hours'
            WHERE id = %s
            """,
            (completed_job[0],),
        )
        cur.execute(
            """
            UPDATE jobs
            SET created_at = statement_timestamp() - INTERVAL '120 seconds'
            WHERE id = %s
            RETURNING created_at
            """,
            (older_queued_job[0],),
        )
        oldest_created_at = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE jobs
            SET created_at = statement_timestamp() - INTERVAL '30 seconds'
            WHERE id = %s
            """,
            (newer_queued_job[0],),
        )

    metrics = get_queue_metrics()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXTRACT(
                EPOCH FROM (statement_timestamp() - %s::timestamptz)
            )::double precision
            """,
            (oldest_created_at,),
        )
        expected_age = cur.fetchone()[0]

    assert isinstance(metrics["oldest_queued_age_seconds"], float)
    assert abs(metrics["oldest_queued_age_seconds"] - expected_age) < 1


def test_metrics_reflect_retry_completion_and_dead_letter_transitions(
    isolated_database,
):
    retry_job = _enqueue(max_attempts=3)
    retry_claim = claim("worker-retry")
    assert fail(
        retry_job[0],
        "worker-retry",
        retry_claim[3],
        "temporary failure",
    )

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET visible_at = statement_timestamp() + INTERVAL '1 hour'
            WHERE id = %s
            """,
            (retry_job[0],),
        )

    after_retry = get_queue_metrics()
    assert after_retry["total_jobs"] == 1
    assert after_retry["queued_jobs"] == 1
    assert after_retry["eligible_jobs"] == 0

    completed_job = _enqueue()
    completed_claim = claim("worker-complete")
    assert complete(completed_job[0], "worker-complete", completed_claim[3])

    dead_job = _enqueue(max_attempts=1)
    dead_claim = claim("worker-dead")
    assert fail(
        dead_job[0],
        "worker-dead",
        dead_claim[3],
        "permanent failure",
    )

    after_terminal_transitions = get_queue_metrics()
    assert after_terminal_transitions["total_jobs"] == 3
    assert after_terminal_transitions["queued_jobs"] == 1
    assert after_terminal_transitions["running_jobs"] == 0
    assert after_terminal_transitions["completed_jobs"] == 1
    assert after_terminal_transitions["dead_jobs"] == 1
    assert after_terminal_transitions["eligible_jobs"] == 0

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET visible_at = statement_timestamp() - INTERVAL '1 second'
            WHERE id = %s
            """,
            (retry_job[0],),
        )

    assert get_queue_metrics()["eligible_jobs"] == 1


def test_connection_errors_propagate(monkeypatch):
    def raise_connection_error():
        raise RuntimeError("Database unavailable")

    monkeypatch.setattr(metrics_module, "get_connection", raise_connection_error)

    with pytest.raises(RuntimeError, match="Database unavailable"):
        get_queue_metrics()


def test_query_errors_propagate(monkeypatch):
    class BrokenCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, query):
            raise RuntimeError("Query failed")

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def cursor(self):
            return BrokenCursor()

    monkeypatch.setattr(metrics_module, "get_connection", FakeConnection)

    with pytest.raises(RuntimeError, match="Query failed"):
        get_queue_metrics()
