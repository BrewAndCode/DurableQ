import random
from datetime import datetime

from psycopg.types.json import Jsonb

from app.database import get_connection


def enqueue(
    job_type: str,
    payload: dict,
    idempotency_key: str | None = None,
    max_attempts: int = 3,
    run_at: datetime | None = None,
) -> tuple[int, str, str]:

    if not isinstance(job_type, str) or not job_type.strip():
        raise ValueError("job_type must be a non-empty string")

    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    if (
        isinstance(max_attempts, bool)
        or not isinstance(max_attempts, int)
        or max_attempts < 1
    ):
        raise ValueError("max_attempts must be a positive integer")

    if idempotency_key is not None and (
        not isinstance(idempotency_key, str) or not idempotency_key.strip()
    ):
        raise ValueError("idempotency_key must be a non-empty string")

    if run_at is not None and (
        not isinstance(run_at, datetime)
        or run_at.tzinfo is None
        or run_at.utcoffset() is None
    ):
        raise ValueError("run_at must be a timezone-aware datetime")

    query = """
        INSERT INTO jobs (
            job_type,
            payload,
            idempotency_key,
            max_attempts,
            visible_at
        )
        VALUES (%s, %s, %s, %s, COALESCE(%s, NOW()))
        ON CONFLICT (idempotency_key)
        WHERE idempotency_key IS NOT NULL
        DO NOTHING
        RETURNING id, job_type, status;
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            query,
            (
                job_type.strip(),
                Jsonb(payload),
                idempotency_key,
                max_attempts,
                run_at,
            ),
        )

        job = cur.fetchone()

        if job is None and idempotency_key is not None:
            cur.execute(
                """
                SELECT id, job_type, status
                FROM jobs
                WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            )
            job = cur.fetchone()

    if job is None:
        raise RuntimeError("Could not retrieve existing idempotent job")

    return job


def claim(
    worker_id: str,
    visibility_timeout_seconds: int = 60,
) -> tuple | None:

    if not isinstance(worker_id, str) or not worker_id.strip():
        raise ValueError("worker_id must be a non-empty string")

    if (
        isinstance(visibility_timeout_seconds, bool)
        or not isinstance(visibility_timeout_seconds, int)
        or visibility_timeout_seconds < 1
    ):
        raise ValueError("visibility_timeout_seconds must be a positive integer")

    query = """
        WITH exhausted AS (
            UPDATE jobs
            SET
                status = 'dead',
                worker_id = NULL,
                updated_at = NOW(),
                last_error = COALESCE(
                    last_error,
                    'Maximum attempts exhausted after lease expiration'
                )
            WHERE status IN ('queued', 'running')
              AND visible_at <= NOW()
              AND attempts >= max_attempts
            RETURNING id
        ),
        candidate AS (
            SELECT id
            FROM jobs
            WHERE status IN ('queued', 'running')
              AND visible_at <= NOW()
              AND attempts < max_attempts
            ORDER BY visible_at, id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs
        SET
            status = 'running',
            attempts = attempts + 1,
            worker_id = %s,
            visible_at = NOW() + (%s * INTERVAL '1 second'),
            updated_at = NOW()
        WHERE id = (SELECT id FROM candidate)
        RETURNING id, job_type, payload, attempts, visible_at;
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            query,
            (worker_id.strip(), visibility_timeout_seconds),
        )
        job = cur.fetchone()

    return job


def heartbeat(
    job_id: int,
    worker_id: str,
    attempt: int,
    visibility_timeout_seconds: int = 60,
) -> bool:

    if isinstance(job_id, bool) or not isinstance(job_id, int) or job_id < 1:
        raise ValueError("job_id must be a positive integer")

    if not isinstance(worker_id, str) or not worker_id.strip():
        raise ValueError("worker_id must be a non-empty string")

    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ValueError("attempt must be a positive integer")

    if (
        isinstance(visibility_timeout_seconds, bool)
        or not isinstance(visibility_timeout_seconds, int)
        or visibility_timeout_seconds < 1
    ):
        raise ValueError("visibility_timeout_seconds must be a positive integer")

    query = """
        UPDATE jobs
        SET
            visible_at = NOW() + (%s * INTERVAL '1 second'),
            updated_at = NOW()
        WHERE id = %s
          AND attempts = %s
          AND status = 'running'
          AND worker_id = %s;
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            query,
            (
                visibility_timeout_seconds,
                job_id,
                attempt,
                worker_id.strip(),
            ),
        )
        updated = cur.rowcount == 1

    return updated


def complete(
    job_id: int,
    worker_id: str,
    attempt: int,
) -> bool:

    if isinstance(job_id, bool) or not isinstance(job_id, int) or job_id < 1:
        raise ValueError("job_id must be a positive integer")

    if not isinstance(worker_id, str) or not worker_id.strip():
        raise ValueError("worker_id must be a non-empty string")

    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ValueError("attempt must be a positive integer")

    query = """
        UPDATE jobs
        SET
            status = 'completed',
            completed_at = NOW(),
            worker_id = NULL,
            updated_at = NOW()
        WHERE id = %s
          AND attempts = %s
          AND status = 'running'
          AND worker_id = %s;
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            query,
            (job_id, attempt, worker_id.strip()),
        )
        updated = cur.rowcount == 1

    return updated


def fail(
    job_id: int,
    worker_id: str,
    attempt: int,
    error: str,
    base_delay_seconds: int = 2,
    max_delay_seconds: int = 300,
) -> bool:

    if isinstance(job_id, bool) or not isinstance(job_id, int) or job_id < 1:
        raise ValueError("job_id must be a positive integer")

    if not isinstance(worker_id, str) or not worker_id.strip():
        raise ValueError("worker_id must be a non-empty string")

    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ValueError("attempt must be a positive integer")

    if not isinstance(error, str) or not error.strip():
        raise ValueError("error must be a non-empty string")

    if (
        isinstance(base_delay_seconds, bool)
        or not isinstance(base_delay_seconds, int)
        or base_delay_seconds < 1
    ):
        raise ValueError("base_delay_seconds must be a positive integer")

    if (
        isinstance(max_delay_seconds, bool)
        or not isinstance(max_delay_seconds, int)
        or max_delay_seconds < base_delay_seconds
    ):
        raise ValueError("max_delay_seconds must be >= base_delay_seconds")

    exponential_delay = min(
        max_delay_seconds,
        base_delay_seconds * (2 ** min(attempt - 1, 30)),
    )

    retry_delay = random.uniform(
        exponential_delay / 2,
        exponential_delay,
    )

    query = """
        UPDATE jobs
        SET
            status = CASE
                WHEN attempts >= max_attempts THEN 'dead'
                ELSE 'queued'
            END,
            visible_at = CASE
                WHEN attempts >= max_attempts THEN NOW()
                ELSE NOW() + (%s * INTERVAL '1 second')
            END,
            worker_id = NULL,
            last_error = %s,
            updated_at = NOW()
        WHERE id = %s
          AND attempts = %s
          AND status = 'running'
          AND worker_id = %s;
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            query,
            (
                retry_delay,
                error,
                job_id,
                attempt,
                worker_id.strip(),
            ),
        )
        updated = cur.rowcount == 1

    return updated
