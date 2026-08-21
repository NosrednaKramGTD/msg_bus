"""Example handler for the mb_test queue.

Used in tests to verify the happy path (validate and handle succeed).
"""

from msg_bus.handlers.base import BaseHandler


class Handler(BaseHandler):
    """Handler that accepts every message."""

    queue_name = "mb_test"

    def handle(self, message: dict) -> None:
        """Message will always be handled."""
        return None
