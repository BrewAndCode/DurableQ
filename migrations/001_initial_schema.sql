CREATE TABLE IF NOT EXISTS jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    job_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'queued',

    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,

    visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    idempotency_key TEXT,

    worker_id TEXT,
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT valid_job_status
        CHECK (status IN ('queued', 'running', 'completed', 'dead')),

    CONSTRAINT valid_attempts
        CHECK (attempts >= 0 AND attempts <= max_attempts),

    CONSTRAINT valid_max_attempts
        CHECK (max_attempts >= 1),

    CONSTRAINT valid_job_type
        CHECK (length(trim(job_type)) > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
ON jobs (idempotency_key)
WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_available
ON jobs (visible_at, id)
WHERE status IN ('queued', 'running');
