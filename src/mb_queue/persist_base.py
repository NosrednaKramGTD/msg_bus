"""Abstract base for queue persistence backends.

Defines the interface for creating queues, enqueueing/dequeueing messages,
archiving, deleting, and retrieving metrics. Implementations (e.g. PersistPGMQ)
provide concrete storage.
"""

from abc import ABC, abstractmethod

from pgmq import Message

from mb_queue.queue_model_dto import DataDTO


class PersistBase(ABC):
    """Abstract base class for queue persistence.

    Implementations must provide queue CRUD, send/receive, archive/delete,
    and metrics. Optional kwargs (e.g. visibility_timeout) are backend-specific.
    """

    @abstractmethod
    def create_queue(self, queue_name: str, options: dict[str, any] | None = None) -> None:
        """Create a new queue if it does not exist. Options are backend-specific."""
        pass

    @abstractmethod
    def list_queues(self) -> list[str]:
        """Return the names of all existing queues."""
        pass

    @abstractmethod
    def destroy_queue(self, queue_name: str) -> None:
        """Delete the queue and its data."""
        pass

    @abstractmethod
    def purge_queue(self, queue_name: str) -> int:
        """Remove all messages from the queue. Returns the number purged."""
        pass

    @abstractmethod
    def enqueue(self, message: DataDTO) -> int:
        """Append a message to the queue. Returns the message ID."""
        pass

    @abstractmethod
    def dequeue(self, queue_name: str, options: dict[str, any] | None = None) -> Message | None:
        """Read one message from the queue (e.g. with visibility timeout). Returns None if empty."""
        pass

    @abstractmethod
    def delete(self, queue_name: str, id: int) -> None:
        """Permanently delete the message with the given ID from the queue."""
        pass

    @abstractmethod
    def archive(self, queue_name: str, id: int) -> None:
        """Move the message from the main queue to the archive."""
        pass

    @abstractmethod
    def metrics(self, queue_name: str) -> dict:
        """Return metrics for the queue (e.g. total, visible, archived counts)."""
        pass

    @abstractmethod
    def enqueue_error(self, message: Message, message_id: int, queue_name: str) -> int | None:
        """Re-enqueue the message with error metadata and remove the original.

        Used for dead-letter style handling. Returns the new message ID or None on failure.
        """
        pass
