"""Base handler interface for queue messages.

Each queue can have a handler module that defines handle (required) and
validate (optional). The processor loads handlers by queue name and calls
validate then handle on each message.
"""

from abc import ABC, abstractmethod


class BaseHandler(ABC):
    """Abstract base for per-queue message handlers.

    Subclasses may override validate to check the message before handling.
    handle is required and performs the actual work (e.g. call an API, update DB).

    Handlers receive a dict ``{"data": ..., "meta": ...}``, not a backend envelope.
    """

    def validate(self, message: dict) -> None:
        """Optionally validate the message; raise if invalid. Default is a no-op."""
        return None

    @abstractmethod
    def handle(self, message: dict) -> None:
        """Process the message. Raise on failure to trigger error re-enqueue."""
