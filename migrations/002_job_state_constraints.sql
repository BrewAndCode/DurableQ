ALTER TABLE jobs
ADD CONSTRAINT valid_worker_assignment
CHECK (
    (
        status = 'running'
        AND worker_id IS NOT NULL
        AND length(trim(worker_id)) > 0
    )
    OR
    (
        status != 'running'
        AND worker_id IS NULL
    )
);

ALTER TABLE jobs
ADD CONSTRAINT valid_completion_timestamp
CHECK (
    (
        status = 'completed'
        AND completed_at IS NOT NULL
    )
    OR
    (
        status != 'completed'
        AND completed_at IS NULL
    )
);

ALTER TABLE jobs
ADD CONSTRAINT valid_payload
CHECK (jsonb_typeof(payload) = 'object');
