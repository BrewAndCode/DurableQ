#!/usr/bin/env python3
"""Run repeatable DurableQ enqueue and worker benchmarks."""

from __future__ import annotations

import argparse
import ipaddress
import json
import logging
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg.conninfo import conninfo_to_dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_ENQUEUE_SIZES = (1_000, 5_000, 10_000)
DEFAULT_WORKER_CONCURRENCY = (1, 5, 10, 20)
DEFAULT_REPETITIONS = 3
DEFAULT_WARMUP_JOBS = 100
DEFAULT_WORKER_JOBS = 5_000
DEFAULT_WORK_TIMEOUT_SECONDS = 300
WORKER_POLL_INTERVAL_SECONDS = 0.005
WORKER_VISIBILITY_TIMEOUT_SECONDS = 300
DATABASE_OBSERVER_POLL_INTERVAL_SECONDS = 0.01
BENCHMARK_JOB_TYPE = "demo.log"
BENCHMARK_PAYLOAD = {"message": "DurableQ reproducible benchmark payload v1"}
DATABASE_SETTINGS = (
    "shared_buffers",
    "work_mem",
    "effective_cache_size",
    "max_connections",
    "synchronous_commit",
    "fsync",
    "full_page_writes",
    "wal_compression",
    "max_wal_size",
    "checkpoint_timeout",
)


def _normalize_loopback_host(host: str) -> str:
    host = host.strip().lower().rstrip(".")
    if host == "localhost":
        return host
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ValueError(
            "Benchmark connections must target localhost or a loopback IP."
        ) from exc
    if not address.is_loopback:
        raise ValueError(
            "Benchmark connections must target localhost or a loopback IP."
        )
    return address.compressed


def validate_benchmark_database_url(
    database_url: str | None,
    allowed_hosts: str | None,
) -> tuple[str, dict[str, str]]:
    """Fail closed unless the target is an allowlisted local benchmark DB."""
    if not database_url or not database_url.strip():
        raise ValueError("BENCHMARK_DATABASE_URL must be configured.")
    if not allowed_hosts or not allowed_hosts.strip():
        raise ValueError(
            "BENCHMARK_DATABASE_ALLOWED_HOSTS must list the local test host."
        )

    try:
        connection_info = conninfo_to_dict(database_url)
    except psycopg.ProgrammingError as exc:
        raise ValueError("BENCHMARK_DATABASE_URL is malformed.") from exc

    if connection_info.get("service") or os.getenv("PGSERVICE"):
        raise ValueError("Service-based PostgreSQL connections are not allowed.")

    raw_hosts = connection_info.get("host", "")
    hosts = raw_hosts.split(",")
    if len(hosts) != 1 or not hosts[0].strip():
        raise ValueError("Benchmark URL must specify exactly one local host.")

    url_host = _normalize_loopback_host(hosts[0])
    approved_hosts = {
        _normalize_loopback_host(host) for host in allowed_hosts.split(",")
    }
    if url_host not in approved_hosts:
        raise ValueError(
            "Benchmark URL host is not listed in "
            "BENCHMARK_DATABASE_ALLOWED_HOSTS."
        )

    hostaddr = connection_info.get("hostaddr") or os.getenv("PGHOSTADDR")
    if hostaddr:
        hostaddrs = hostaddr.split(",")
        if len(hostaddrs) != 1:
            raise ValueError("Benchmark URL must specify one local host address.")
        address = _normalize_loopback_host(hostaddrs[0])
        if address not in approved_hosts:
            raise ValueError(
                "Benchmark URL host address is not explicitly allowlisted."
            )

    database_name = connection_info.get("dbname", "")
    if not re.fullmatch(r"durableq_bench_[a-zA-Z0-9_]+", database_name):
        raise ValueError(
            "Benchmark database name must start with durableq_bench_."
        )

    if re.search(r"production|development|(?:^|_)(?:prod|dev)(?:_|$)", database_name, re.I):
        raise ValueError("Production or development databases are not allowed.")

    return database_name, connection_info


def _set_database_url(database_url: str) -> None:
    # Set this only after validating the explicit benchmark URL. app.database
    # loads .env at import time, but load_dotenv() does not override this value.
    os.environ["DATABASE_URL"] = database_url


def _connection_factory():
    from app.database import get_connection

    return get_connection


def _assert_fresh_database(database_url: str, expected_name: str) -> None:
    with psycopg.connect(database_url, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT current_database(), current_setting('server_version'), "
            "current_setting('server_version_num')::integer, "
            "to_regclass('public.jobs'), to_regclass('public.schema_migrations')"
        )
        database_name, server_version, version_num, jobs_table, migration_table = (
            cur.fetchone()
        )
    if database_name != expected_name:
        raise RuntimeError("Connected database name does not match the benchmark URL.")
    if not 160000 <= version_num < 170000:
        raise RuntimeError(
            f"PostgreSQL 16 is required; target reports {server_version}."
        )
    if jobs_table is not None or migration_table is not None:
        raise RuntimeError(
            "Benchmark database is not fresh: jobs or schema_migrations already exists. "
            "Create a new durableq_bench_* database."
        )


def _run_query(query: str, params: tuple | None = None):
    get_connection = _connection_factory()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def _truncate_jobs() -> float:
    get_connection = _connection_factory()
    started = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")
    return time.perf_counter() - started


def _enqueue_jobs(count: int, run_label: str) -> list[int]:
    from app.queue import enqueue

    job_ids = []
    for number in range(count):
        job_id, _, _ = enqueue(
            job_type=BENCHMARK_JOB_TYPE,
            payload=BENCHMARK_PAYLOAD,
            idempotency_key=f"{run_label}-{number}",
        )
        job_ids.append(job_id)

    if len(set(job_ids)) != count:
        raise RuntimeError("Enqueue API returned duplicate job IDs.")
    return job_ids


def _verify_enqueued_jobs(expected_count: int) -> dict[str, int]:
    rows = _run_query(
        """
        SELECT count(*), count(DISTINCT idempotency_key),
               count(*) FILTER (WHERE status = 'queued')
        FROM jobs
        """
    )
    row_count, unique_keys, queued_count = rows[0]
    result = {
        "rows": int(row_count),
        "unique_idempotency_keys": int(unique_keys),
        "queued_jobs": int(queued_count),
        "lost_jobs": max(0, expected_count - int(row_count)),
        "unexpected_jobs": max(0, int(row_count) - expected_count),
    }
    if row_count != expected_count or unique_keys != expected_count:
        raise RuntimeError(f"Enqueue correctness check failed: {result}")
    if queued_count != expected_count:
        raise RuntimeError(
            "Enqueue state correctness check failed: "
            f"expected {expected_count} queued jobs, got {queued_count}."
        )
    return result


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("Cannot calculate percentiles for an empty sample.")
    index = (len(ordered) - 1) * percentile / 100
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = index - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _run_worker_pool(
    job_count: int,
    concurrency: int,
    timeout_seconds: int,
) -> dict:
    from app.handlers import handle_demo_log
    from app.worker import Worker

    workers = [
        Worker(
            worker_id=f"benchmark-{uuid4().hex}-{index}",
            handlers={BENCHMARK_JOB_TYPE: handle_demo_log},
            poll_interval_seconds=WORKER_POLL_INTERVAL_SECONDS,
            visibility_timeout_seconds=WORKER_VISIBILITY_TIMEOUT_SECONDS,
        )
        for index in range(concurrency)
    ]
    threads = [
        threading.Thread(
            target=worker.run_forever,
            name=f"durableq-benchmark-worker-{index}",
        )
        for index, worker in enumerate(workers)
    ]

    get_connection = _connection_factory()
    observer = get_connection()
    observer.autocommit = True
    started = time.perf_counter()
    completed_at = None
    shutdown_seconds = 0.0
    try:
        for thread in threads:
            thread.start()

        with observer.cursor() as cur:
            while True:
                cur.execute(
                    """
                    SELECT count(*) FILTER (WHERE status = 'completed'),
                           count(*) FILTER (WHERE status = 'dead')
                    FROM jobs
                    """
                )
                completed_count, dead_count = cur.fetchone()
                if dead_count:
                    raise RuntimeError(
                        f"Worker benchmark produced {dead_count} dead jobs."
                    )
                if completed_count == job_count:
                    completed_at = time.perf_counter()
                    break
                if time.perf_counter() - started >= timeout_seconds:
                    raise TimeoutError(
                        f"Only {completed_count}/{job_count} jobs completed."
                    )
                time.sleep(DATABASE_OBSERVER_POLL_INTERVAL_SECONDS)
    finally:
        shutdown_started = time.perf_counter()
        for worker in workers:
            worker.stop()
        for thread in threads:
            thread.join(timeout=30)
        shutdown_seconds = time.perf_counter() - shutdown_started
        observer.close()

    if any(thread.is_alive() for thread in threads):
        raise TimeoutError("A DurableQ worker thread did not stop after completion.")
    if completed_at is None:
        raise RuntimeError("Worker completion time was not recorded.")

    elapsed_seconds = completed_at - started
    rows = _run_query(
        "SELECT id, status, attempts, created_at, completed_at FROM jobs ORDER BY id"
    )
    actual_ids = {row[0] for row in rows}
    attempts = [row[2] for row in rows]
    latencies_ms = [
        (row[4] - row[3]).total_seconds() * 1000
        for row in rows
        if row[4] is not None
    ]
    completed_rows = sum(row[1] == "completed" for row in rows)
    lost_jobs = max(0, job_count - len(actual_ids))
    duplicate_claims = sum(max(0, attempt - 1) for attempt in attempts)
    incorrect_attempts = sum(attempt != 1 for attempt in attempts)
    incorrect_states = len(rows) - completed_rows

    correctness = {
        "expected_jobs": job_count,
        "observed_jobs": len(rows),
        "completed_jobs": completed_rows,
        "lost_jobs": lost_jobs,
        "duplicate_claims": duplicate_claims,
        "jobs_with_attempt_count_other_than_one": incorrect_attempts,
        "incorrect_states": incorrect_states,
    }
    if (
        len(rows) != job_count
        or len(actual_ids) != job_count
        or completed_rows != job_count
        or incorrect_attempts
        or incorrect_states
        or len(latencies_ms) != job_count
    ):
        raise RuntimeError(f"Worker correctness check failed: {correctness}")

    return {
        "elapsed_seconds": elapsed_seconds,
        "completed_jobs_per_second": job_count / elapsed_seconds,
        "shutdown_seconds_excluded": shutdown_seconds,
        "latency_ms": {
            "p50": _percentile(latencies_ms, 50),
            "p95": _percentile(latencies_ms, 95),
            "p99": _percentile(latencies_ms, 99),
            "mean": statistics.fmean(latencies_ms),
        },
        "correctness": correctness,
    }


def _measure_enqueue(
    sizes: tuple[int, ...], repetitions: int, warmup_jobs: int
) -> list[dict]:
    measurements = []
    for size in sizes:
        runs = []
        for repetition in range(1, repetitions + 1):
            setup_seconds = _truncate_jobs()
            warmup_started = time.perf_counter()
            _enqueue_jobs(warmup_jobs, f"warmup-enqueue-{uuid4().hex}")
            warmup_seconds = time.perf_counter() - warmup_started
            setup_seconds += _truncate_jobs()

            started = time.perf_counter()
            _enqueue_jobs(size, f"enqueue-{uuid4().hex}")
            elapsed_seconds = time.perf_counter() - started
            correctness = _verify_enqueued_jobs(size)
            cleanup_seconds = _truncate_jobs()
            result = {
                "repetition": repetition,
                "jobs": size,
                "elapsed_seconds": elapsed_seconds,
                "jobs_per_second": size / elapsed_seconds,
                "setup_seconds_excluded": setup_seconds,
                "warmup_seconds_excluded": warmup_seconds,
                "cleanup_seconds_excluded": cleanup_seconds,
                "correctness": correctness,
            }
            runs.append(result)
            print(
                f"enqueue jobs={size} run={repetition}/{repetitions} "
                f"jobs_per_second={result['jobs_per_second']:.2f}",
                flush=True,
            )
        measurements.append(
            {
                "jobs": size,
                "runs": runs,
                "median_jobs_per_second": statistics.median(
                    run["jobs_per_second"] for run in runs
                ),
            }
        )
    return measurements


def _measure_workers(
    worker_jobs: int,
    concurrencies: tuple[int, ...],
    repetitions: int,
    warmup_jobs: int,
    timeout_seconds: int,
) -> list[dict]:
    measurements = []
    for concurrency in concurrencies:
        runs = []
        for repetition in range(1, repetitions + 1):
            _truncate_jobs()
            warmup_enqueue_started = time.perf_counter()
            _enqueue_jobs(warmup_jobs, f"warmup-worker-{uuid4().hex}")
            warmup_enqueue_seconds = time.perf_counter() - warmup_enqueue_started
            warmup_run = _run_worker_pool(
                warmup_jobs,
                concurrency,
                timeout_seconds,
            )
            warmup_seconds = (
                warmup_enqueue_seconds
                + warmup_run["elapsed_seconds"]
                + warmup_run["shutdown_seconds_excluded"]
            )
            setup_seconds = _truncate_jobs()

            preload_started = time.perf_counter()
            _enqueue_jobs(worker_jobs, f"worker-{uuid4().hex}")
            preload_seconds = time.perf_counter() - preload_started

            measured = _run_worker_pool(
                worker_jobs,
                concurrency,
                timeout_seconds,
            )
            cleanup_seconds = _truncate_jobs()
            result = {
                "repetition": repetition,
                "workers": concurrency,
                "jobs": worker_jobs,
                **measured,
                "warmup_seconds_excluded": warmup_seconds,
                "setup_seconds_excluded": setup_seconds,
                "preload_seconds_excluded": preload_seconds,
                "cleanup_seconds_excluded": cleanup_seconds,
            }
            runs.append(result)
            print(
                f"workers={concurrency} jobs={worker_jobs} "
                f"run={repetition}/{repetitions} "
                f"jobs_per_second={result['completed_jobs_per_second']:.2f} "
                f"p95_ms={result['latency_ms']['p95']:.2f}",
                flush=True,
            )

        measurements.append(
            {
                "workers": concurrency,
                "jobs_per_run": worker_jobs,
                "runs": runs,
                "median_completed_jobs_per_second": statistics.median(
                    run["completed_jobs_per_second"] for run in runs
                ),
                "median_latency_ms": {
                    percentile: statistics.median(
                        run["latency_ms"][percentile] for run in runs
                    )
                    for percentile in ("p50", "p95", "p99")
                },
            }
        )
    return measurements


def _host_metadata() -> dict:
    processor = platform.processor() or platform.machine()
    total_memory = None
    if sys.platform == "darwin":
        model = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            check=False,
        )
        if model.returncode == 0 and model.stdout.strip():
            processor = model.stdout.strip()
        memory = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True,
            text=True,
            check=False,
        )
        if memory.returncode == 0 and memory.stdout.strip().isdigit():
            total_memory = int(memory.stdout.strip())
    elif Path("/proc/meminfo").exists():
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                total_memory = int(line.split()[1]) * 1024
                break

    docker_server_version = os.getenv("BENCHMARK_DOCKER_SERVER_VERSION")
    docker = shutil.which("docker")
    if docker and not docker_server_version:
        version = subprocess.run(
            [docker, "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if version.returncode == 0:
            docker_server_version = version.stdout.strip() or None

    runner = {
        "os": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": processor,
        "logical_cpu_count": os.cpu_count(),
        "host_memory_bytes": total_memory,
        "python_version": platform.python_version(),
        "docker_server_version": docker_server_version,
    }
    host_metadata_json = os.getenv("BENCHMARK_HOST_METADATA_JSON")
    if host_metadata_json:
        try:
            host_metadata = json.loads(host_metadata_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError("BENCHMARK_HOST_METADATA_JSON is invalid JSON.") from exc
        if not isinstance(host_metadata, dict):
            raise RuntimeError("BENCHMARK_HOST_METADATA_JSON must contain an object.")
    else:
        host_metadata = None

    return {"benchmark_process": runner, "host": host_metadata}


def _postgres_metadata() -> dict:
    settings = _run_query(
        """
        SELECT name, setting, unit
        FROM pg_settings
        WHERE name = ANY(%s)
        ORDER BY name
        """,
        (list(DATABASE_SETTINGS),),
    )
    version = _run_query("SELECT version(), current_setting('server_version')")[0]
    container_name = os.getenv("BENCHMARK_POSTGRES_CONTAINER")
    resource_limits = os.getenv("BENCHMARK_POSTGRES_RESOURCE_LIMITS")
    docker = shutil.which("docker")
    if docker and container_name and not resource_limits:
        inspected = subprocess.run(
            [
                docker,
                "inspect",
                "--format",
                "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}",
                container_name,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if inspected.returncode == 0:
            resource_limits = inspected.stdout.strip()

    return {
        "version_text": version[0],
        "server_version": version[1],
        "settings": {
            name: {"setting": value, "unit": unit}
            for name, value, unit in settings
        },
        "container_name": container_name,
        "container_memory_and_cpu_limits": resource_limits,
        "data_directory_storage": os.getenv(
            "BENCHMARK_DATA_DIRECTORY_STORAGE", "not reported"
        ),
    }


def _validate_workload_args(args: argparse.Namespace) -> None:
    if args.repetitions < 3:
        raise SystemExit("--repetitions must be at least 3.")
    if args.warmup_jobs < 1 or args.worker_jobs < 1:
        raise SystemExit("Warm-up and worker job counts must be positive.")
    if args.work_timeout_seconds < 1:
        raise SystemExit("--work-timeout-seconds must be positive.")
    if any(size < 1 for size in args.enqueue_sizes):
        raise SystemExit("Enqueue sizes must be positive.")
    if len(set(args.enqueue_sizes)) != len(args.enqueue_sizes):
        raise SystemExit("Enqueue sizes must be unique.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--enqueue-sizes",
        nargs="+",
        type=int,
        default=DEFAULT_ENQUEUE_SIZES,
        help="Enqueue workload sizes (default: 1000 5000 10000).",
    )
    parser.add_argument(
        "--worker-jobs",
        type=int,
        default=DEFAULT_WORKER_JOBS,
        help="Identical preloaded job count for each worker run (default: 5000).",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=DEFAULT_REPETITIONS,
        help="Measured repetitions per case; must be at least 3.",
    )
    parser.add_argument(
        "--warmup-jobs",
        type=int,
        default=DEFAULT_WARMUP_JOBS,
        help="Unmeasured warm-up jobs per case (default: 100).",
    )
    parser.add_argument(
        "--work-timeout-seconds",
        type=int,
        default=DEFAULT_WORK_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmarks/results.json"),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow overwriting the output file.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    _validate_workload_args(args)
    if args.output.exists() and not args.force:
        raise SystemExit(
            f"Output already exists: {args.output}. Choose another --output or pass --force."
        )

    database_url = os.getenv("BENCHMARK_DATABASE_URL")
    allowed_hosts = os.getenv("BENCHMARK_DATABASE_ALLOWED_HOSTS")
    try:
        database_name, _ = validate_benchmark_database_url(
            database_url,
            allowed_hosts,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    assert database_url is not None
    _assert_fresh_database(database_url, database_name)
    _set_database_url(database_url)

    logging.basicConfig(level=logging.WARNING)
    from app.migrate import get_applied_migrations, run_migrations

    migration_started = time.perf_counter()
    migrations_applied = run_migrations()
    schema_setup_seconds = time.perf_counter() - migration_started
    applied_versions = sorted(get_applied_migrations())

    # Worker retry loops log an exception per failed database connection. Keep
    # benchmark output bounded; the observer still fails the run on DB errors.
    logging.getLogger("app.worker").setLevel(logging.CRITICAL)
    logging.getLogger("app.handlers").setLevel(logging.WARNING)

    try:
        enqueue_results = _measure_enqueue(
            tuple(args.enqueue_sizes),
            args.repetitions,
            args.warmup_jobs,
        )
        worker_results = _measure_workers(
            args.worker_jobs,
            DEFAULT_WORKER_CONCURRENCY,
            args.repetitions,
            args.warmup_jobs,
            args.work_timeout_seconds,
        )
    finally:
        final_cleanup_seconds = _truncate_jobs()

    postgres_metadata = _postgres_metadata()
    result = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "database_name": database_name,
        "database_host": "loopback allowlist",
        "environment": {
            **_host_metadata(),
            "postgresql": postgres_metadata,
        },
        "workload": {
            "job_type": BENCHMARK_JOB_TYPE,
            "handler": "app.handlers.handle_demo_log",
            "payload": BENCHMARK_PAYLOAD,
            "payload_json_bytes": len(json.dumps(BENCHMARK_PAYLOAD).encode()),
            "enqueue_sizes": list(args.enqueue_sizes),
            "worker_jobs_per_run": args.worker_jobs,
            "worker_concurrency": list(DEFAULT_WORKER_CONCURRENCY),
            "repetitions": args.repetitions,
            "warmup_jobs_per_case": args.warmup_jobs,
            "worker_poll_interval_seconds": WORKER_POLL_INTERVAL_SECONDS,
            "worker_visibility_timeout_seconds": WORKER_VISIBILITY_TIMEOUT_SECONDS,
            "database_observer_poll_interval_seconds": DATABASE_OBSERVER_POLL_INTERVAL_SECONDS,
            "migrations_applied_during_setup": migrations_applied,
            "schema_versions_after_setup": applied_versions,
            "schema_setup_seconds_excluded": schema_setup_seconds,
            "final_cleanup_seconds_excluded": final_cleanup_seconds,
            "latency_definition": "jobs.completed_at - jobs.created_at using PostgreSQL timestamps",
            "worker_throughput_definition": "all jobs completed, measured from worker thread startup to database observer confirmation",
            "enqueue_throughput_definition": "public enqueue() calls completed per second; per-job connection and transaction costs included",
            "correctness_definition": "every expected job row completed and had attempts=1; attempts>1 counts duplicate claim attempts",
        },
        "enqueue_throughput": enqueue_results,
        "worker_concurrency": worker_results,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(f"Migrations applied: {migrations_applied}")
    print(f"PostgreSQL: {postgres_metadata['server_version']}")
    print(f"Results written to {args.output}")
    print(f"Final benchmark-table cleanup: {final_cleanup_seconds:.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
