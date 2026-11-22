import logging

import pytest

from app.handlers import HANDLERS, handle_demo_log
from app.worker_cli import create_worker


def test_demo_log_handler_validates_and_does_not_log_payload(caplog):
    caplog.set_level(logging.INFO)
    private_message = "sensitive demo text"

    handle_demo_log({"message": private_message})

    assert "Processed demo.log job" in caplog.text
    assert private_message not in caplog.text


@pytest.mark.parametrize(
    "payload",
    [None, [], {}, {"message": 3}, {"message": ""}, {"message": "   "}],
)
def test_demo_log_handler_rejects_invalid_payload(payload):
    with pytest.raises((TypeError, ValueError)):
        handle_demo_log(payload)


def test_create_worker_uses_supported_environment_settings(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@db/durableq")
    monkeypatch.setenv("WORKER_ID", "worker-test")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0.25")
    monkeypatch.setenv("VISIBILITY_TIMEOUT_SECONDS", "12")

    worker = create_worker()

    assert worker.worker_id.startswith("worker-test-")
    assert worker.handlers is HANDLERS
    assert worker.poll_interval_seconds == 0.25
    assert worker.visibility_timeout_seconds == 12


def test_create_worker_generates_a_unique_worker_id(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@db/durableq")
    monkeypatch.setenv("WORKER_ID", "worker-test")

    first = create_worker()
    second = create_worker()

    assert first.worker_id != second.worker_id
