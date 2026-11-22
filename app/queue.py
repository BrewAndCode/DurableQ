import random
import re
from datetime import datetime

from psycopg.types.json import Jsonb

from app.database import get_connection

MAX_LAST_ERROR_LENGTH = 256

_DATABASE_CREDENTIALS = re.compile(r"(?i)\b(postgres(?:ql)?://)[^/\s]+@")
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[a-z0-9._~+/=-]+")
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?ix)"
    r"(?P<prefix>['\"]?(?:password|passwd|pwd|secret|token|access[_-]?token|"
    r"refresh[_-]?token|api[_-]?key|authorization|credential)['\"]?\s*[:=]\s*)"
    r"(?:"
    r"(?P<quote>['\"])(?P<quoted>.*?)(?P=quote)"
    r"|(?P<unquoted>[^'\"\s,;}\]]+)"
    r")"
)
_JWT = re.compile(
    r"\beyJ[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\b"
)


def _sanitize_error_summary(error: str) -> str:
    """Bound stored diagnostics and redact common credential formats."""
    bounded_input = error[: MAX_LAST_ERROR_LENGTH * 4]
    without_controls = re.sub(r"[\x00-\x1f\x7f]+", " ", bounded_input)
    summary = " ".join(without_controls.split())
    summary = _DATABASE_CREDENTIALS.sub(r"\1[REDACTED]@", summary)
    summary = _BEARER_TOKEN.sub("Bearer [REDACTED]", summary)
    summary = _JWT.sub("[REDACTED_JWT]", summary)

    def redact_assignment(match):
        quote = match.group("quote")
        if quote:
            return f"{match.group('prefix')}{quote}[REDACTED]{quote}"
        return f"{match.group('prefix')}[REDACTED]"

    summary = _SENSITIVE_ASSIGNMENT.sub(redact_assignment, summary)
    summary = summary[:MAX_LAST_ERROR_LENGTH]
    return summary or "Unspecified job failure"


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
        WITH exhausted_candidates AS MATERIALIZED (
            SELECT id
            FROM jobs
            WHERE status IN ('queued', 'running')
              AND visible_at <= NOW()
              AND attempts >= max_attempts
            ORDER BY visible_at, id
            LIMIT 100
            FOR UPDATE SKIP LOCKED
        ),
        exhausted AS (
            UPDATE jobs AS exhausted_job
            SET
                status = 'dead',
                worker_id = NULL,
                updated_at = NOW(),
                last_error = COALESCE(
                    exhausted_job.last_error,
                    'Maximum attempts exhausted after lease expiration'
                )
            FROM exhausted_candidates
            WHERE exhausted_job.id = exhausted_candidates.id
            RETURNING exhausted_job.id
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


def _lock_valid_lease(cur, job_id: int, worker_id: str, attempt: int) -> bool:
    """Lock the current ownership row, then check its deadline using DB time."""
    cur.execute(
        """
        SELECT id
        FROM jobs
        WHERE id = %s
          AND attempts = %s
          AND status = 'running'
          AND worker_id = %s
        FOR UPDATE;
        """,
        (job_id, attempt, worker_id),
    )
    if cur.fetchone() is None:
        return False

    # Use a new statement so this check runs after any wait for the row lock.
    cur.execute(
        """
        SELECT visible_at > clock_timestamp()
        FROM jobs
        WHERE id = %s
          AND attempts = %s
          AND status = 'running'
          AND worker_id = %s;
        """,
        (job_id, attempt, worker_id),
    )
    lease = cur.fetchone()
    return lease is not None and lease[0]


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
            visible_at = clock_timestamp() + (%s * INTERVAL '1 second'),
            updated_at = clock_timestamp()
        WHERE id = %s
          AND attempts = %s
          AND status = 'running'
          AND worker_id = %s
          AND visible_at > clock_timestamp();
    """

    with get_connection() as conn, conn.cursor() as cur:
        if not _lock_valid_lease(cur, job_id, worker_id.strip(), attempt):
            return False

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
            completed_at = clock_timestamp(),
            worker_id = NULL,
            updated_at = clock_timestamp()
        WHERE id = %s
          AND attempts = %s
          AND status = 'running'
          AND worker_id = %s
          AND visible_at > clock_timestamp();
    """

    with get_connection() as conn, conn.cursor() as cur:
        if not _lock_valid_lease(cur, job_id, worker_id.strip(), attempt):
            return False

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

    error = _sanitize_error_summary(error)

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
                WHEN attempts >= max_attempts THEN clock_timestamp()
                ELSE clock_timestamp() + (%s * INTERVAL '1 second')
            END,
            worker_id = NULL,
            last_error = %s,
            updated_at = clock_timestamp()
        WHERE id = %s
          AND attempts = %s
          AND status = 'running'
          AND worker_id = %s
          AND visible_at > clock_timestamp();
    """

    with get_connection() as conn, conn.cursor() as cur:
        if not _lock_valid_lease(cur, job_id, worker_id.strip(), attempt):
            return False

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
