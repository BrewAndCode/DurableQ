"""Small example handlers for running the DurableQ demo worker."""

import logging

logger = logging.getLogger(__name__)


def handle_demo_log(payload: dict) -> None:
    """Validate and log a demo event without logging its contents."""
    if not isinstance(payload, dict):
        raise TypeError("demo.log payload must be an object")

    message = payload.get("message")
    if not isinstance(message, str) or not message.strip():
        raise ValueError("demo.log payload requires a non-empty message string")

    logger.info("Processed demo.log job")


HANDLERS = {
    "demo.log": handle_demo_log,
}
