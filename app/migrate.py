from pathlib import Path
import re

from app.database import get_connection

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

LEGACY_CONSTRAINTS = {
    "002_job_state_constraints": {
        "valid_worker_assignment",
        "valid_completion_timestamp",
        "valid_payload",
    }
}

REQUIRED_COLUMNS = {
    "id": ("int8", None, "NO", None, "YES", "ALWAYS"),
    "job_type": ("varchar", 100, "NO", None, "NO", None),
    "payload": ("jsonb", None, "NO", None, "NO", None),
    "status": ("varchar", 20, "NO", "'queued'::character varying", "NO", None),
    "attempts": ("int4", None, "NO", "0", "NO", None),
    "max_attempts": ("int4", None, "NO", "3", "NO", None),
    "visible_at": ("timestamptz", None, "NO", "now()", "NO", None),
    "idempotency_key": ("text", None, "YES", None, "NO", None),
    "worker_id": ("text", None, "YES", None, "NO", None),
    "last_error": ("text", None, "YES", None, "NO", None),
    "created_at": ("timestamptz", None, "NO", "now()", "NO", None),
    "updated_at": ("timestamptz", None, "NO", "now()", "NO", None),
    "completed_at": ("timestamptz", None, "YES", None, "NO", None),
}

BASE_CONSTRAINTS = {
    "jobs_pkey": ("p", "PRIMARY KEY (id)"),
    "valid_job_status": (
        "c",
        "CHECK (status::text = ANY (ARRAY['queued'::character varying, "
        "'running'::character varying, 'completed'::character varying, "
        "'dead'::character varying]::text[]))",
    ),
    "valid_attempts": (
        "c",
        "CHECK (attempts >= 0 AND attempts <= max_attempts)",
    ),
    "valid_max_attempts": ("c", "CHECK (max_attempts >= 1)"),
    "valid_job_type": (
        "c",
        "CHECK (length(TRIM(BOTH FROM job_type)) > 0)",
    ),
}

LEASE_CONSTRAINTS = {
    "valid_worker_assignment": (
        "c",
        "CHECK (status::text = 'running'::text AND worker_id IS NOT NULL "
        "AND length(TRIM(BOTH FROM worker_id)) > 0 OR status::text <> "
        "'running'::text AND worker_id IS NULL)",
    ),
    "valid_completion_timestamp": (
        "c",
        "CHECK (status::text = 'completed'::text AND completed_at IS NOT NULL "
        "OR status::text <> 'completed'::text AND completed_at IS NULL)",
    ),
    "valid_payload": (
        "c",
        "CHECK (jsonb_typeof(payload) = 'object'::text)",
    ),
}

BASE_INDEXES = {
    "jobs_pkey": (("id",), True, True, None),
    "idx_jobs_idempotency": (
        ("idempotency_key",),
        True,
        False,
        "(idempotency_key IS NOT NULL)",
    ),
    "idx_jobs_available": (
        ("visible_at", "id"),
        False,
        False,
        "((status)::text = ANY ((ARRAY['queued'::character varying, "
        "'running'::character varying])::text[]))",
    ),
}

MIGRATION_SCHEMA_REQUIREMENTS = {
    "001_initial_schema": {
        "constraints": BASE_CONSTRAINTS,
        "indexes": BASE_INDEXES,
    },
    "002_job_state_constraints": {
        "constraints": {**BASE_CONSTRAINTS, **LEASE_CONSTRAINTS},
        "indexes": BASE_INDEXES,
    },
}


def _normalize_sql(value: str | None) -> str | None:
    if value is None:
        return None
    return re.sub(r"\s+", " ", value).strip().lower()


def _validate_migration_schema(cur, version: str) -> None:
    requirements = MIGRATION_SCHEMA_REQUIREMENTS.get(version)
    if requirements is None:
        raise RuntimeError(
            f"No schema validation requirements are defined for migration {version}."
        )

    cur.execute("SELECT to_regclass('public.jobs')")
    if cur.fetchone()[0] is None:
        raise RuntimeError(
            f"Schema validation failed for migration {version}: public.jobs is missing."
        )

    errors = []

    cur.execute(
        """
        SELECT column_name, udt_name, character_maximum_length, is_nullable,
               column_default, is_identity, identity_generation
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'jobs';
        """
    )
    columns = {
        row[0]: (row[1], row[2], row[3], _normalize_sql(row[4]), row[5], row[6])
        for row in cur.fetchall()
    }
    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(columns))
    incompatible_columns = sorted(
        name
        for name, expected in REQUIRED_COLUMNS.items()
        if name in columns and columns[name] != expected
    )
    if missing_columns:
        errors.append(f"missing jobs columns: {missing_columns}")
    if incompatible_columns:
        errors.append(f"incompatible jobs columns: {incompatible_columns}")

    cur.execute(
        """
        SELECT conname, contype, convalidated, pg_get_constraintdef(oid, true)
        FROM pg_constraint
        WHERE conrelid = to_regclass('public.jobs');
        """
    )
    constraints = {
        row[0]: (row[1], row[2], _normalize_sql(row[3]))
        for row in cur.fetchall()
    }
    expected_constraints = requirements["constraints"]
    missing_constraints = sorted(set(expected_constraints) - set(constraints))
    incompatible_constraints = sorted(
        name
        for name, (expected_type, expected_definition) in expected_constraints.items()
        if name in constraints
        and constraints[name]
        != (expected_type, True, _normalize_sql(expected_definition))
    )
    if missing_constraints:
        errors.append(f"missing jobs constraints: {missing_constraints}")
    if incompatible_constraints:
        errors.append(f"incompatible or unvalidated jobs constraints: {incompatible_constraints}")

    cur.execute(
        """
        SELECT index_class.relname, index_info.indisunique,
               index_info.indisprimary, index_info.indisvalid,
               index_info.indisready,
               ARRAY(
                   SELECT attribute.attname
                   FROM unnest(index_info.indkey)
                        WITH ORDINALITY AS key_column(attnum, ordinal)
                   JOIN pg_attribute AS attribute
                     ON attribute.attrelid = index_info.indrelid
                    AND attribute.attnum = key_column.attnum
                   WHERE key_column.ordinal <= index_info.indnkeyatts
                   ORDER BY key_column.ordinal
               ) AS key_columns,
               pg_get_expr(index_info.indpred, index_info.indrelid) AS predicate
        FROM pg_index AS index_info
        JOIN pg_class AS index_class ON index_class.oid = index_info.indexrelid
        WHERE index_info.indrelid = to_regclass('public.jobs');
        """
    )
    indexes = {
        row[0]: (row[1], row[2], row[3], row[4], tuple(row[5]), _normalize_sql(row[6]))
        for row in cur.fetchall()
    }
    expected_indexes = requirements["indexes"]
    missing_indexes = sorted(set(expected_indexes) - set(indexes))
    incompatible_indexes = sorted(
        name
        for name, (expected_columns, expected_unique, expected_primary, expected_predicate)
        in expected_indexes.items()
        if name in indexes
        and indexes[name]
        != (
            expected_unique,
            expected_primary,
            True,
            True,
            expected_columns,
            _normalize_sql(expected_predicate),
        )
    )
    if missing_indexes:
        errors.append(f"missing jobs indexes: {missing_indexes}")
    if incompatible_indexes:
        errors.append(f"incompatible or invalid jobs indexes: {incompatible_indexes}")

    if errors:
        raise RuntimeError(
            f"Schema validation failed for migration {version}: "
            + "; ".join(errors)
        )


def ensure_migration_table():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)


def get_applied_migrations() -> set[str]:
    ensure_migration_table()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT version FROM schema_migrations")
        return {row[0] for row in cur.fetchall()}


def discover_migrations() -> dict[str, Path]:
    migrations = {}

    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        version = path.stem

        if version in migrations:
            raise ValueError(f"Duplicate migration version: {version}")

        migrations[version] = path

    return migrations


def _verify_legacy_constraints(cur, version: str) -> bool:
    required = LEGACY_CONSTRAINTS[version]

    cur.execute(
        """
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = to_regclass('public.jobs')
          AND conname = ANY(%s);
    """,
        (list(required),),
    )

    existing = {row[0] for row in cur.fetchall()}

    if existing == required:
        return True

    if existing:
        missing = required - existing
        raise RuntimeError(
            f"Partially applied migration {version}. "
            f"Missing constraints: {sorted(missing)}"
        )

    return False


def run_migrations() -> list[str]:
    ensure_migration_table()

    migrations = discover_migrations()
    applied_now = []

    with get_connection() as conn, conn.cursor() as cur:
        # Prevent two processes from running migrations simultaneously.
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            ("durableq_schema_migrations",),
        )

        cur.execute("SELECT version FROM schema_migrations")
        applied = {row[0] for row in cur.fetchall()}

        for version, path in migrations.items():
            if version in applied:
                _validate_migration_schema(cur, version)
                continue

            sql = path.read_text(encoding="utf-8")

            # CREATE TABLE IF NOT EXISTS cannot bring an existing jobs table
            # up to the initial schema. Validate it before running migration
            # 001 so an incompatible legacy table is never accepted or
            # partially modified as an adoption.
            if version == "001_initial_schema":
                cur.execute("SELECT to_regclass('public.jobs')")
                if cur.fetchone()[0] is not None:
                    _validate_migration_schema(cur, version)

            if version in LEGACY_CONSTRAINTS:
                already_present = _verify_legacy_constraints(cur, version)

                if not already_present:
                    cur.execute(sql)
            else:
                cur.execute(sql)

            # Validate inside the same transaction, before recording success.
            # A schema mismatch therefore rolls back both migration DDL and
            # its version row.
            _validate_migration_schema(cur, version)

            cur.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES (%s)
                """,
                (version,),
            )

            applied_now.append(version)

    return applied_now
