"""Exception-test handler for the exception_test queue.

Used to verify that validation and handling failures are caught, logged,
and (for handle) re-enqueued with error metadata.
"""

from mb_queue.handlers.base import BaseHandler


class Handler(BaseHandler):
    """Handler that always raises in validate and handle for testing error paths."""

    queue_name = "exception_test"

    def validate(self, message: dict) -> None:
        """Raise ValueError to test validation error handling."""
        raise ValueError("Testing Validate Exception")

    def handle(self, message: dict) -> None:
        """Raise to test handle error handling and dead-letter re-enqueue."""
        raise Exception("Testing Handle Exception")
