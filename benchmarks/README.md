# DurableQ performance benchmarks

`run_benchmarks.py` measures the public enqueue API and the real `Worker` plus
`app.handlers.handle_demo_log` implementation against PostgreSQL 16. It does
not use a mock queue or a synthetic worker. The demo handler is intentionally
small, so worker results measure DurableQ and PostgreSQL overhead for a
lightweight handler; they are not a prediction for CPU-heavy or network-bound
application handlers.

## Safety and isolation

Run the benchmarks only against a newly created, disposable PostgreSQL 16
instance and a database whose name starts with `durableq_bench_`. The runner
fails closed unless `BENCHMARK_DATABASE_ALLOWED_HOSTS` explicitly lists the
URL's loopback host. It checks the selected database is PostgreSQL 16 and has
no existing `public.jobs` or `public.schema_migrations` objects before applying
migrations. It does not read `.env`, use `DATABASE_URL`, create databases, or
drop databases. It truncates only the guarded benchmark database between cases
and at the end. Never point it at a shared, development, or production server,
even if a database name matches the required prefix.

The commands below use a new PostgreSQL container with `tmpfs` and no named or
persistent Docker volume. The benchmark client shares that container's network
namespace and connects over loopback. The database port is not published to
the host. Cleanup stops only the named temporary benchmark container and does
not stop or remove the project's Compose services or volumes.

## Prerequisites

- Docker with a working engine and permission to run containers.
- Python 3.13 and the project's runtime dependencies installed (for example,
  `.venv/bin/python` from the repository root).
- Enough Docker memory for a PostgreSQL 16 container and a benchmark client.

## Run the full benchmark

Run from the repository root with shell command tracing disabled. This builds a
temporary benchmark image from the current checkout, starts isolated
PostgreSQL 16, applies migrations, warms up, and runs all measured cases. The
database container uses a generated URL-safe password that is not printed.

```sh
set +x
BENCH_SUFFIX="$(date +%Y%m%d%H%M%S)"
BENCH_CONTAINER="durableq-bench-pg16-${BENCH_SUFFIX}"
BENCH_DB="durableq_bench_${BENCH_SUFFIX}"
BENCH_IMAGE="durableq-benchmark:${BENCH_SUFFIX}"
BENCH_PASSWORD="$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))')"
DOCKER_SERVER_VERSION="$(docker version --format '{{.Server.Version}}')"
HOST_METADATA_JSON="$(.venv/bin/python -c 'import json,os,platform,subprocess; model=platform.processor() or platform.machine(); memory=None; model=subprocess.check_output(["sysctl","-n","machdep.cpu.brand_string"],text=True).strip() if platform.system()=="Darwin" else model; memory=int(subprocess.check_output(["sysctl","-n","hw.memsize"],text=True).strip()) if platform.system()=="Darwin" else None; print(json.dumps({"os":platform.platform(),"machine":platform.machine(),"cpu_model":model,"logical_cpu_count":os.cpu_count(),"host_memory_bytes":memory}))')"

cleanup_benchmark() {
  docker stop "$BENCH_CONTAINER" >/dev/null 2>&1 || true
  docker image rm "$BENCH_IMAGE" >/dev/null 2>&1 || true
  unset BENCH_PASSWORD
}
trap cleanup_benchmark EXIT

docker run --rm --detach \
  --name "$BENCH_CONTAINER" \
  --tmpfs /var/lib/postgresql/data:rw,size=2g \
  --env POSTGRES_DB="$BENCH_DB" \
  --env POSTGRES_USER=durableq_bench \
  --env POSTGRES_PASSWORD="$BENCH_PASSWORD" \
  postgres:16 >/dev/null

for attempt in $(seq 1 60); do
  if docker exec "$BENCH_CONTAINER" pg_isready -U durableq_bench -d "$BENCH_DB" >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "PostgreSQL did not become ready within 60 seconds." >&2
    exit 1
  fi
  sleep 1
done

docker build --tag "$BENCH_IMAGE" .
POSTGRES_LIMITS="$(docker inspect --format 'memory_bytes={{.HostConfig.Memory}} nano_cpus={{.HostConfig.NanoCpus}}' "$BENCH_CONTAINER")"
docker run --rm \
  --network "container:${BENCH_CONTAINER}" \
  --mount "type=bind,source=${PWD}/benchmarks,target=/app/benchmarks" \
  --workdir /app \
  --env "BENCHMARK_DATABASE_URL=postgresql://durableq_bench:${BENCH_PASSWORD}@127.0.0.1:5432/${BENCH_DB}" \
  --env BENCHMARK_DATABASE_ALLOWED_HOSTS=127.0.0.1 \
  --env "BENCHMARK_POSTGRES_CONTAINER=${BENCH_CONTAINER}" \
  --env "BENCHMARK_POSTGRES_RESOURCE_LIMITS=${POSTGRES_LIMITS}" \
  --env BENCHMARK_DATA_DIRECTORY_STORAGE='tmpfs at /var/lib/postgresql/data (2 GiB)' \
  --env "BENCHMARK_DOCKER_SERVER_VERSION=${DOCKER_SERVER_VERSION}" \
  --env "BENCHMARK_HOST_METADATA_JSON=${HOST_METADATA_JSON}" \
  "$BENCH_IMAGE" python /app/benchmarks/run_benchmarks.py --output /app/benchmarks/results.json

docker stop "$BENCH_CONTAINER" >/dev/null
unset BENCH_PASSWORD
```

The program checks the connected server is PostgreSQL 16, initializes the
schema with the repository migration runner, and records the applied migration
versions. The default workload has three measured repetitions each for 1,000,
5,000, and 10,000 enqueues, then three 5,000-job worker runs at each concurrency
level (1, 5, 10, and 20). Each case has a 100-job warm-up. Enqueue, preload,
schema setup, warm-up, cleanup, and worker shutdown time are excluded from the
corresponding measured throughput interval. Each enqueue still includes the
public API's per-job connection and transaction cost.

Worker latency is `completed_at - created_at`, measured using PostgreSQL
timestamps. This is enqueue-to-completion latency and includes how long a job
waits while the remainder of its batch is enqueued. Worker throughput starts
when worker threads begin and ends when a separate database connection sees
all jobs complete; that observer queries every 10 ms, and its query overhead is
included in measured throughput. Workers use a 5 ms idle poll interval and a
300 second visibility timeout. Percentiles use linear interpolation. The JSON
includes each measured repetition, median throughput, median latency, host and
runner hardware/runtime details, PostgreSQL version, and selected server
settings.

Correctness checks require all expected rows to exist with distinct IDs, every
worker job to be completed, and every worker job to have exactly one attempt.
Missing rows, unexpected rows, duplicate claim attempts, dead jobs, or an
incomplete run cause failure instead of a successful results file.

The runner refuses to overwrite an existing output file; pass `--force` only
when intentionally replacing evidence. Smaller diagnostic workloads can use
the runner's command-line options, but the defaults above are the release
comparison suite. At least three measured repetitions are required.

## Results and interpretation

`results.json` is generated after a completed run and contains individual
measurements and medians. Compare results only when workload, host, PostgreSQL
version/settings, storage, and resource limits match. The database uses
`tmpfs`, so measurements exclude durable disk-write latency and are not
production storage performance. They are also sensitive to Docker resource
allocation, host load, CPU throttling, PostgreSQL configuration, and the
lightweight demo handler.

### Observed run: 2026-11-22

This run used Python 3.13.18, PostgreSQL 16.16, Docker Engine 29.9.2, an Apple
M1 host (8 logical CPUs, 8 GiB RAM), and a Docker Linux VM exposing 8 logical
CPUs and about 3.8 GiB RAM. PostgreSQL used its image defaults, including
`shared_buffers=128 MiB`, `work_mem=4 MiB`, `max_connections=100`,
`synchronous_commit=on`, and `fsync=on`. The benchmark database was on a 2 GiB
tmpfs. Exact environment and full precision observations are in
[`results.json`](results.json).

Enqueue throughput (jobs/sec):

| Jobs | Run 1 | Run 2 | Run 3 | Median |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 378.36 | 414.49 | 381.38 | 381.38 |
| 5,000 | 442.06 | 465.47 | 470.46 | 465.47 |
| 10,000 | 471.69 | 469.85 | 462.08 | 469.85 |

Worker throughput (completed jobs/sec) and median end-to-end latency:

| Workers | Run rates | Median jobs/sec | p50 ms | p95 ms | p99 ms |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 164.63, 162.92, 163.28 | 163.28 | 20,940.06 | 29,671.12 | 30,421.08 |
| 5 | 411.54, 396.81, 373.87 | 396.81 | 11,563.87 | 12,449.01 | 12,578.09 |
| 10 | 341.40, 350.75, 346.32 | 346.32 | 12,601.50 | 14,229.31 | 14,393.37 |
| 20 | 354.23, 340.15, 346.80 | 346.80 | 12,495.22 | 14,225.00 | 14,362.46 |

For this lightweight handler, five workers had the best median throughput.
Increasing to 10 or 20 workers did not improve it, which is consistent with
database/connection and claim overhead becoming the limiting work in this
setup; these measurements alone do not isolate a specific PostgreSQL
bottleneck. Latency includes the sequential batch-enqueue period before worker
startup, so it should be compared only with the same batch and enqueue method.
Every measured worker repetition completed all 5,000 jobs with `attempts=1`:
zero missing jobs, duplicate claim attempts, dead jobs, or incorrect states.
All enqueue runs produced exactly the requested number of queued rows and
unique idempotency keys.
