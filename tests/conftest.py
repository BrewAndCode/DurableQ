import os

import pytest
from dotenv import load_dotenv


def pytest_configure():
    load_dotenv()

    test_database_url = os.getenv("TEST_DATABASE_URL")

    if not test_database_url:
        raise pytest.UsageError("TEST_DATABASE_URL must be configured")

    os.environ["DATABASE_URL"] = test_database_url
