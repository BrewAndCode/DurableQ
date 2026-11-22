
import os
from pathlib import Path
from uuid import uuid4

import pytest
import psycopg
from psycopg import Error, connect
from psycopg.conninfo import conninfo_to_dict, make_conninfo

import app.migrate as migrate_module
import app.database as database_module
from app.database import get_connection
from app.migrate import (
    discover_migrations,
    ensure_migration_table,
    get_applied_migrations,
    run_migrations,
)


EXPECTED_MIGRATIONS = {
    "001_initial_schema",
    "002_job_state_constraints",
}

EXPECTED_CONSTRAINTS = {
    "jobs_pkey",
    "valid_job_status",
    "valid_attempts",
    "valid_max_attempts",
    "valid_job_type",
    "valid_worker_assignment",
    "valid_completion_timestamp",
    "valid_payload",
}

EXPECTED_INDEXES = {
    "jobs_pkey",
    "idx_jobs_idempotency",
    "idx_jobs_available",
}


def test_database_connections_set_finite_connect_and_network_timeouts(
    monkeypatch,
):
    captured = {}
    sentinel = object()

    def fake_connect(conninfo):
        captured["conninfo"] = conninfo
        return sentinel

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://localhost/durableq_test?options=-c%20application_name%3Dtest-client",
    )
    monkeypatch.setattr(database_module.psycopg, "connect", fake_connect)

    assert database_module.get_connection() is sentinel

    parameters = conninfo_to_dict(captured["conninfo"])
    assert parameters["connect_timeout"] == "5"
    assert parameters["keepalives"] == "1"
    assert parameters["keepalives_idle"] == "5"
    assert parameters["keepalives_interval"] == "2"
    assert parameters["keepalives_count"] == "3"
    assert parameters["tcp_user_timeout"] == "10000"
    assert "application_name=test-client" in parameters["options"]
    assert "statement_timeout=5000" in parameters["options"]
    assert "lock_timeout=2000" in parameters["options"]


def test_statement_timeout_cancels_long_database_query(
    isolated_database, monkeypatch
):
    monkeypatch.setattr(database_module, "STATEMENT_TIMEOUT_MS", 50)

    with database_module.get_connection() as conn, conn.cursor() as cur:
        cur.execute("SHOW statement_timeout")
        assert cur.fetchone()[0] == "50ms"

        with pytest.raises(psycopg.errors.QueryCanceled):
            cur.execute("SELECT pg_sleep(1)")


def _schema_objects():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'public.jobs'::regclass
            """
        )
        constraints = {row[0] for row in cur.fetchall()}

        cur.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = 'jobs'
            """
        )
        indexes = {row[0] for row in cur.fetchall()}

    return constraints, indexes


@pytest.fixture
def isolated_database(monkeypatch):
    test_database_url = os.environ["TEST_DATABASE_URL"]
    database_name = f"durableq_test_migration_{uuid4().hex}"

    with connect(test_database_url, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{database_name}"')

    isolated_url = make_conninfo(
        test_database_url,
        dbname=database_name,
    )

    monkeypatch.setenv("DATABASE_URL", isolated_url)

    try:
        yield isolated_url
    finally:
        with connect(test_database_url, autocommit=True) as conn:
            conn.execute(
                f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)'
            )


def test_get_applied_migrations_returns_set(isolated_database):
    applied = get_applied_migrations()

    assert isinstance(applied, set)


def test_get_applied_migrations_returns_recorded_versions(isolated_database):
    ensure_migration_table()

    version = f"test_migration_{uuid4()}"

    try:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s)",
                (version,),
            )

        assert version in get_applied_migrations()

    finally:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM schema_migrations WHERE version = %s",
                (version,),
            )


def test_discover_migrations_finds_constraint_migration():
    migrations = discover_migrations()

    assert list(migrations) == [
        "001_initial_schema",
        "002_job_state_constraints",
    ]


def test_run_migrations_adopts_existing_constraints(isolated_database):
    schema_sql = (
        Path(__file__).resolve().parents[1] / "app" / "schema.sql"
    ).read_text(encoding="utf-8")
    with get_connection() as conn:
        conn.execute(schema_sql)

    ensure_migration_table()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO schema_migrations (version) VALUES (%s)",
            ("001_initial_schema",),
        )

    applied_now = run_migrations()

    applied = get_applied_migrations()

    assert applied_now == ["002_job_state_constraints"]
    assert applied == EXPECTED_MIGRATIONS


def test_run_migrations_skips_already_applied_migrations(isolated_database):
    seeded = run_migrations()
    first_run = run_migrations()
    second_run = run_migrations()

    assert seeded == ["001_initial_schema", "002_job_state_constraints"]
    assert first_run == []
    assert second_run == []


def test_run_migrations_records_applied_version(isolated_database):
    run_migrations()
    applied = get_applied_migrations()

    assert applied == EXPECTED_MIGRATIONS


def test_run_migrations_applies_constraints_to_clean_schema(
    isolated_database,
):
    initial_schema_sql = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "001_initial_schema.sql"
    ).read_text(encoding="utf-8")

    with get_connection() as conn:
        conn.execute(initial_schema_sql)

    ensure_migration_table()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO schema_migrations (version) VALUES (%s)",
            ("001_initial_schema",),
        )

    first_run = run_migrations()
    second_run = run_migrations()

    assert first_run == ["002_job_state_constraints"]
    assert second_run == []

    constraints, _ = _schema_objects()

    assert {
        "valid_worker_assignment",
        "valid_completion_timestamp",
        "valid_payload",
    }.issubset(constraints)


def test_run_migrations_rejects_incomplete_legacy_schema_without_data_loss(
    isolated_database,
):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE jobs (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                job_type TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "INSERT INTO jobs (job_type) VALUES (%s) RETURNING id",
            ("legacy-preserved",),
        )
        legacy_job_id = cur.fetchone()[0]

    with pytest.raises(RuntimeError, match="Schema validation failed"):
        run_migrations()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT job_type FROM jobs WHERE id = %s", (legacy_job_id,))
        assert cur.fetchone() == ("legacy-preserved",)

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'jobs'
            """
        )
        assert {row[0] for row in cur.fetchall()} == {"id", "job_type"}

        cur.execute("SELECT version FROM schema_migrations")
        assert cur.fetchall() == []


def test_run_migrations_validates_already_recorded_schema(isolated_database):
    assert run_migrations() == [
        "001_initial_schema",
        "002_job_state_constraints",
    ]

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("ALTER TABLE jobs DROP CONSTRAINT valid_payload")
        cur.execute("ALTER TABLE jobs ADD CONSTRAINT valid_payload CHECK (true)")

    with pytest.raises(
        RuntimeError,
        match="incompatible or unvalidated jobs constraints",
    ):
        run_migrations()

    assert get_applied_migrations() == EXPECTED_MIGRATIONS


def test_run_migrations_initializes_completely_empty_database(
    isolated_database,
):
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.jobs')")
        assert cur.fetchone()[0] is None

    applied_now = run_migrations()

    assert applied_now == [
        "001_initial_schema",
        "002_job_state_constraints",
    ]
    assert get_applied_migrations() == EXPECTED_MIGRATIONS

    constraints, indexes = _schema_objects()
    assert EXPECTED_CONSTRAINTS.issubset(constraints)
    assert EXPECTED_INDEXES.issubset(indexes)

    second_run = run_migrations()

    assert second_run == []
    assert get_applied_migrations() == EXPECTED_MIGRATIONS
    assert _schema_objects() == (constraints, indexes)


def test_run_migrations_adopts_legacy_schema_without_versions(
    isolated_database,
):
    schema_sql = (
        Path(__file__).resolve().parents[1] / "app" / "schema.sql"
    ).read_text(encoding="utf-8")

    with get_connection() as conn:
        conn.execute(schema_sql)

    assert get_applied_migrations() == set()

    applied_now = run_migrations()

    assert applied_now == [
        "001_initial_schema",
        "002_job_state_constraints",
    ]
    assert get_applied_migrations() == EXPECTED_MIGRATIONS

    constraints, indexes = _schema_objects()
    assert EXPECTED_CONSTRAINTS.issubset(constraints)
    assert EXPECTED_INDEXES.issubset(indexes)


def test_failed_migration_rolls_back_ddl_and_version_record(
    isolated_database,
    monkeypatch,
    tmp_path,
):
    migration_dir = tmp_path / "migrations"
    migration_dir.mkdir()
    migration_path = migration_dir / "001_intentional_failure.sql"
    migration_path.write_text(
        """
        DO $$
        BEGIN
            CREATE TABLE migration_rollback_probe (id INTEGER PRIMARY KEY);
            RAISE EXCEPTION 'intentional migration failure';
        END
        $$;
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr(migrate_module, "MIGRATIONS_DIR", migration_dir)

    with pytest.raises(Error, match="intentional migration failure"):
        run_migrations()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.migration_rollback_probe')")
        assert cur.fetchone()[0] is None

        cur.execute(
            "SELECT version FROM schema_migrations WHERE version = %s",
            ("001_intentional_failure",),
        )
        assert cur.fetchone() is None
