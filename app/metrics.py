from app.database import get_connection


def get_queue_metrics() -> dict[str, int | float | None]:
    """Return aggregate queue health metrics without exposing job data."""
    query = """
        SELECT
            COUNT(*) AS total_jobs,
            COUNT(*) FILTER (WHERE status = 'queued') AS queued_jobs,
            COUNT(*) FILTER (WHERE status = 'running') AS running_jobs,
            COUNT(*) FILTER (WHERE status = 'completed') AS completed_jobs,
            COUNT(*) FILTER (WHERE status = 'dead') AS dead_jobs,
            COUNT(*) FILTER (
                WHERE status = 'queued'
                  AND visible_at <= statement_timestamp()
            ) AS eligible_jobs,
            EXTRACT(
                EPOCH FROM (
                    statement_timestamp()
                    - MIN(created_at) FILTER (WHERE status = 'queued')
                )
            )::double precision AS oldest_queued_age_seconds
        FROM jobs;
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()

    (
        total_jobs,
        queued_jobs,
        running_jobs,
        completed_jobs,
        dead_jobs,
        eligible_jobs,
        oldest_queued_age_seconds,
    ) = row

    return {
        "total_jobs": int(total_jobs),
        "queued_jobs": int(queued_jobs),
        "running_jobs": int(running_jobs),
        "completed_jobs": int(completed_jobs),
        "dead_jobs": int(dead_jobs),
        "eligible_jobs": int(eligible_jobs),
        "oldest_queued_age_seconds": (
            float(oldest_queued_age_seconds)
            if oldest_queued_age_seconds is not None
            else None
        ),
    }
