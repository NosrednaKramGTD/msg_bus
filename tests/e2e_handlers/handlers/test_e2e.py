"""Handler for queue test_e2e used by E2E CLI tests.

Validates and handles messages; no-op so E2E can assert via CLI exit codes and queue emptiness.
"""

from msg_bus.handlers.base import BaseHandler


class Handler(BaseHandler):
    """Handler for queue test_e2e; validates and handles (no-op) for E2E flow."""

    def __init__(self) -> None:
        """Initialize the handler."""
        pass

    def validate(self, message: dict) -> None:
        """Accept any message."""
        pass

    def handle(self, message: dict) -> None:
        """No-op; E2E asserts via process exit code and queue empty after run."""
        pass
