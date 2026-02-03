"""test handler for testing.

Queue to post to and verify handlers are working.
"""

from msg_bus.handlers.base import BaseHandler


class Handler(BaseHandler):
    """Handler that always raises in validate and handle for testing error paths."""

    queue_name = "exception_test"

    def __init__(self) -> None:
        """Initialize the handler."""
        self.queue_name = "mb_test"

    def validate(self, message: dict) -> None:
        """Validation will always pass."""
        return None

    def handle(self, message: dict) -> None:
        """Message will always be handled."""
        return None
