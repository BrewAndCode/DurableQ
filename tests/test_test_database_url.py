import psycopg
import pytest

from tests.conftest import validate_test_database_url

LOCAL_TEST_HOST = "localhost"


def test_accepts_durableq_test_url():
    assert (
        validate_test_database_url(
            "postgresql://localhost/durableq_test",
            LOCAL_TEST_HOST,
        )
        == "durableq_test"
    )


def test_accepts_approved_ci_postgres_service_host():
    assert (
        validate_test_database_url(
            "postgresql://127.0.0.1/durableq_test",
            "127.0.0.1",
        )
        == "durableq_test"
    )


def test_accepts_url_with_credentials_port_and_query_parameters():
    database_url = (
        "postgresql://queue_user:p%40ssword@localhost:5433/"
        "durableq_test?sslmode=require&connect_timeout=5"
    )

    assert (
        validate_test_database_url(database_url, LOCAL_TEST_HOST)
        == "durableq_test"
    )


@pytest.mark.parametrize("database_url", [None, "", "   "])
def test_rejects_missing_url(database_url):
    with pytest.raises(pytest.UsageError, match="TEST_DATABASE_URL"):
        validate_test_database_url(database_url, LOCAL_TEST_HOST)


@pytest.mark.parametrize(
    "database_url",
    ["not-a-postgresql-url", "postgresql://[invalid-host/durableq_test"],
)
def test_rejects_malformed_url(database_url):
    with pytest.raises(pytest.UsageError, match="valid PostgreSQL URL"):
        validate_test_database_url(database_url, LOCAL_TEST_HOST)


def test_rejects_url_without_database_name():
    with pytest.raises(pytest.UsageError, match="include a dedicated test"):
        validate_test_database_url("postgresql://localhost/", LOCAL_TEST_HOST)


@pytest.mark.parametrize(
    "database_url",
    ["postgresql:///durableq_test", "postgresql://[::1/durableq_test"],
)
def test_rejects_missing_or_malformed_host(database_url):
    with pytest.raises(pytest.UsageError):
        validate_test_database_url(database_url, LOCAL_TEST_HOST)


def test_requires_explicit_allowed_host_list():
    with pytest.raises(pytest.UsageError, match="TEST_DATABASE_ALLOWED_HOSTS"):
        validate_test_database_url("postgresql://localhost/durableq_test", None)


def test_rejects_unrecognized_host():
    with pytest.raises(pytest.UsageError, match="not listed"):
        validate_test_database_url(
            "postgresql://postgres-test.internal/durableq_test",
            LOCAL_TEST_HOST,
        )


def test_rejects_production_like_host_even_if_allowlisted():
    with pytest.raises(pytest.UsageError, match="Production-like"):
        validate_test_database_url(
            "postgresql://prod-db.internal/durableq_test",
            "prod-db.internal",
        )


def test_rejects_hostaddr_that_bypasses_allowlisted_host():
    with pytest.raises(pytest.UsageError, match="host address is not listed"):
        validate_test_database_url(
            "postgresql://localhost/durableq_test?hostaddr=192.0.2.10",
            LOCAL_TEST_HOST,
        )


@pytest.mark.parametrize(
    "database_name",
    ["durableq", "durableq_dev"],
)
def test_rejects_database_without_test_marker(database_name):
    with pytest.raises(pytest.UsageError):
        validate_test_database_url(
            f"postgresql://localhost/{database_name}",
            LOCAL_TEST_HOST,
        )


@pytest.mark.parametrize(
    "database_name",
    [
        "durableq_prod",
        "durableq_production",
        "durableq_test_prod",
        "durableq_production_test",
        "durableqproduction_test",
        "durableq_test_development",
        "durableqdevelopment_test",
    ],
)
def test_rejects_production_or_development_database_names(database_name):
    with pytest.raises(pytest.UsageError, match="production or development"):
        validate_test_database_url(
            f"postgresql://localhost/{database_name}",
            LOCAL_TEST_HOST,
        )


def test_decodes_url_encoded_database_name():
    assert (
        validate_test_database_url(
            "postgresql://localhost/durableq%5Ftest",
            LOCAL_TEST_HOST,
        )
        == "durableq_test"
    )


def test_validator_does_not_connect_to_postgresql(monkeypatch):
    def fail_if_connected(*args, **kwargs):
        pytest.fail("URL validation must not connect to PostgreSQL")

    monkeypatch.setattr(psycopg, "connect", fail_if_connected)

    assert (
        validate_test_database_url(
            "postgresql://localhost/durableq_test",
            LOCAL_TEST_HOST,
        )
        == "durableq_test"
    )
