# Design decisions and rationale

This note describes tradeoffs visible in the current implementation. The
available project history does not establish every original motivation, so
these points are implementation-based design rationale rather than claims
about the initial decision process.

## PostgreSQL as the queue store

DurableQ stores queue state in PostgreSQL and uses its transactions, indexes,
and row locks for coordination. This keeps job state in the database the
application may already operate. It also means queue availability and
throughput depend on that PostgreSQL service. The implementation does not
provide a Redis backend.

## `FOR UPDATE SKIP LOCKED` for claims

The claim query locks eligible rows and skips rows another claimant has locked.
It updates the selected job to `running`, increments its attempt, assigns the
worker, and establishes a visibility deadline as one database operation. The
handler runs after the claim transaction has ended, so job execution does not
hold the claim row lock.

## At-least-once delivery

If a worker exits after performing an external side effect but before DurableQ
records completion, the visibility lease can expire and the job can be claimed
again. DurableQ therefore offers at-least-once execution, not exactly-once
execution. Handlers that perform external effects should be idempotent or use
their own deduplication protocol where duplicate effects matter.

## Visibility timeout and fencing

`visible_at` is the lease deadline for a running job and the scheduled
availability time for a queued job. Heartbeat extends a live lease. Heartbeat,
completion, and failure updates also check job ID, attempt, status, worker ID,
and lease expiry after locking the ownership row. These checks prevent an old
worker from changing database state after expiry or reclaim; they cannot
reverse external work the old worker already performed.

## Idempotent enqueue

A partial unique index protects non-null idempotency keys. A duplicate enqueue
returns the existing job identity, type, and status. The first stored payload,
attempt limit, and schedule remain in effect. This deduplicates enqueue
requests; it does not make handler execution exactly once.

## Cooperative cancellation

The heartbeat thread signals lease loss through a `threading.Event` to a
handler explicitly wrapped in `CancellableHandler`. This lets cooperative
handlers stop work when they observe the event, while preserving the legacy
one-argument handler interface. Python cannot safely force-stop an arbitrary
thread, and blocked non-cancellable I/O or an event-ignoring handler may
continue. Cancellation also cannot undo an external side effect.
