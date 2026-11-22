import ipaddress
import os
import re
from urllib.parse import urlsplit

import pytest
from psycopg import ProgrammingError
from psycopg.conninfo import conninfo_to_dict
from dotenv import load_dotenv

_TEST_MARKER = re.compile(r"(?:^|[^a-z0-9])test(?:$|[^a-z0-9])", re.IGNORECASE)
_PRODUCTION_MARKER = re.compile(
    r"(?:production|development|(?:^|[^a-z0-9])(?:prod|dev)(?:$|[^a-z0-9]))",
    re.IGNORECASE,
)
_UNSAFE_HOST_MARKER = re.compile(
    r"(?:production|development|prod|dev|live|primary)",
    re.IGNORECASE,
)


def _normalize_test_host(host: str) -> str:
    host = host.strip()
    if not host:
        raise pytest.UsageError("Test database host must not be empty.")

    try:
        normalized = ipaddress.ip_address(host).compressed
    except ValueError:
        normalized = host.rstrip(".").lower()

    if not normalized:
        raise pytest.UsageError("Test database host must not be empty.")

    if _UNSAFE_HOST_MARKER.search(normalized):
        raise pytest.UsageError(
            "Production-like or development hosts are not allowed for tests."
        )

    return normalized


def _parse_allowed_test_hosts(allowed_hosts: str | None) -> set[str]:
    if not isinstance(allowed_hosts, str) or not allowed_hosts.strip():
        raise pytest.UsageError(
            "TEST_DATABASE_ALLOWED_HOSTS must explicitly list approved test hosts."
        )

    hosts = [_normalize_test_host(host) for host in allowed_hosts.split(",")]
    if any(not host for host in hosts):
        raise pytest.UsageError(
            "TEST_DATABASE_ALLOWED_HOSTS must contain comma-separated hostnames."
        )

    return set(hosts)


def validate_test_database_url(
    database_url: str | None,
    allowed_hosts: str | None,
) -> str:
    """Validate a test PostgreSQL URL against an explicit host allowlist."""
    approved_hosts = _parse_allowed_test_hosts(allowed_hosts)

    if not isinstance(database_url, str) or not database_url.strip():
        raise pytest.UsageError(
            "TEST_DATABASE_URL must point to a dedicated test database "
            "such as durableq_test."
        )

    try:
        parsed_url = urlsplit(database_url)
        if parsed_url.scheme.lower() not in {"postgres", "postgresql"}:
            raise ValueError("unsupported database URL scheme")

        # Accessing port validates its syntax without opening a connection.
        _ = parsed_url.port
        connection_info = conninfo_to_dict(database_url)
    except (ProgrammingError, ValueError) as exc:
        raise pytest.UsageError(
            "TEST_DATABASE_URL must be a valid PostgreSQL URL with a "
            "dedicated test database name."
        ) from exc

    database_name = connection_info.get("dbname")
    if not database_name:
        raise pytest.UsageError(
            "TEST_DATABASE_URL must include a dedicated test database name, "
            "such as durableq_test."
        )

    if connection_info.get("service") or os.getenv("PGSERVICE"):
        raise pytest.UsageError(
            "Service-based PostgreSQL connections are not supported for tests; "
            "specify an allowlisted host directly."
        )

    raw_hosts = connection_info.get("host")
    if not raw_hosts:
        raise pytest.UsageError(
            "TEST_DATABASE_URL must specify a host in TEST_DATABASE_ALLOWED_HOSTS."
        )

    hosts = raw_hosts.split(",")
    if len(hosts) != 1:
        raise pytest.UsageError(
            "TEST_DATABASE_URL must target exactly one approved test host."
        )

    normalized_host = _normalize_test_host(hosts[0])
    if normalized_host not in approved_hosts:
        raise pytest.UsageError(
            "TEST_DATABASE_URL host is not listed in TEST_DATABASE_ALLOWED_HOSTS."
        )

    # libpq's hostaddr can override DNS resolution while leaving an approved
    # host in the URL. Validate it as well whenever it is explicitly supplied.
    raw_hostaddrs = connection_info.get("hostaddr") or os.getenv("PGHOSTADDR")
    if raw_hostaddrs:
        hostaddrs = raw_hostaddrs.split(",")
        if len(hostaddrs) != 1:
            raise pytest.UsageError(
                "TEST_DATABASE_URL must target exactly one approved host address."
            )
        if _normalize_test_host(hostaddrs[0]) not in approved_hosts:
            raise pytest.UsageError(
                "TEST_DATABASE_URL host address is not listed in "
                "TEST_DATABASE_ALLOWED_HOSTS."
            )

    if _PRODUCTION_MARKER.search(database_name):
        raise pytest.UsageError(
            "TEST_DATABASE_URL cannot target a production or development "
            "database. Use a dedicated test database such as durableq_test."
        )

    if not _TEST_MARKER.search(database_name):
        raise pytest.UsageError(
            "TEST_DATABASE_URL database name must include a clear test "
            "marker, such as durableq_test."
        )

    return database_name


def pytest_configure():
    load_dotenv()

    test_database_url = os.getenv("TEST_DATABASE_URL")
    allowed_hosts = os.getenv("TEST_DATABASE_ALLOWED_HOSTS")
    validate_test_database_url(test_database_url, allowed_hosts)

    os.environ["DATABASE_URL"] = test_database_url
