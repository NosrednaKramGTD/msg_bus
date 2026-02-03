"""PostgreSQL-backed queue persistence using PGMQ.

Uses the pgmq library to store queues and messages in PostgreSQL with
visibility timeouts, archiving, and metrics.
"""

import logging
from urllib.parse import urlparse

from pgmq import Message, PGMQueue
from pgmq.decorators import transaction
from pydantic import PostgresDsn

from msg_bus.persist_base import PersistBase
from msg_bus.queue_model_dto import DataDTO

class PersistPGMQ(PersistBase):
    """Queue persistence implementation using PGMQ (PostgreSQL Message Queue).

    Connects via a Postgres DSN and delegates to PGMQueue for create, send,
    read, archive, delete, and metrics. Supports partitioned queues via
    create_queue options.
    """

    def __init__(self, dsn: PostgresDsn | str | None = None) -> None:
        """Connect to PostgreSQL using the given DSN or settings default."""
        raw = dsn or os.getenv("PGMQ_DSN", None)
        # coerce to pydantic PostgresDsn (validates) then parse as a standard URL
        parts = urlparse(str(raw))

        # noinspection PyTypeChecker
        self.queue = PGMQueue(
            host=parts.hostname,
            port=parts.port,
            database=parts.path.lstrip("/"),
            username=parts.username,
            password=parts.password,
            verbose=True,
            log_filename="pgmq.log",
        )
        # Add logger attribute required by @transaction decorator
        self.logger = logging.getLogger(__name__)

    @property
    def pool(self):
        """Expose the queue's connection pool for transaction decorator."""
        return self.queue.pool

    def enqueue(self, message: DataDTO) -> int:
        """Append the message to the queue named in message.meta.queue_name. Returns message ID."""
        payload = message.model_dump()
        message_id = self.queue.send(
            queue=message.meta.queue_name,
            message=payload,
        )
        return message_id

    def dequeue(self, queue_name: str, options: dict[str, any] | None = None) -> Message | None:
        """Read one message from the queue with the given visibility timeout (seconds)."""
        visibility_timeout = options.get("visibility_timeout", 300)
        message = self.queue.read(
            queue=queue_name,
            vt=visibility_timeout,
        )
        return message

    def delete(self, queue_name: str, id: int) -> None:
        """Permanently delete the message with the given ID from the queue."""
        self.queue.delete(
            queue=queue_name,
            msg_id=id,
        )

    def archive(self, queue_name: str, id: int) -> None:
        """Move the message from the main queue to the archive."""
        self.queue.archive(
            queue=queue_name,
            msg_id=id,
        )

    def create_queue(self, queue_name: str, options: dict[str, any] | None = None) -> None:
        """Create a new queue; options may enable partitioning (interval, retention)."""
        options = options or {}
        if options.get("partition", "false").lower() == "true":
            self.queue.create_partitioned_queue(
                queue_name,
                partitions_interval=int(options.get("interval", 1000)),
                retention_interval=int(options.get("retention", 1000000)),
            )
            return
        self.queue.create_queue(queue_name)

    def destroy_queue(self, queue_name: str) -> None:
        """Drop the queue and its data."""
        self.queue.drop_queue(queue_name)

    def purge_queue(self, queue_name: str) -> int:
        """Remove all messages from the specified queue."""
        purged_count = self.queue.purge(queue_name)
        return purged_count

    def list_queues(self) -> list[str]:
        """List all existing queues."""
        return self.queue.list_queues()

    def metrics(self, queue_name: str) -> dict:
        """Get metrics for the specified queue."""
        return self.queue.metrics(queue_name)

    def close(self) -> None:
        """Close the connection pool; call when done to avoid shutdown warnings."""
        if hasattr(self.queue, "pool") and self.queue.pool:
            self.queue.pool.close()

    @transaction
    def enqueue_error(
        self,
        message: dict,
        message_id: int,
        queue: PGMQueue,
        visibility_timeout: int = 5,
        conn=None,
    ) -> int:
        """Re-enqueue the message (with error metadata), delete the original, set VT.

        Used as a dead-letter path: the same message is re-sent so it can be
        retried later with a longer visibility timeout.
        """
        queue_name = message["meta"]["queue_name"]
        error_message_id = self.queue.send(
            queue=queue_name,
            message=message,
            conn=conn,
        )
        self.queue.delete(
            queue=queue_name,
            msg_id=message_id,
            conn=conn,
        )
        self.queue.set_vt(
            queue=queue_name,
            msg_id=error_message_id,
            vt=visibility_timeout,
            conn=conn,
        )
        return error_message_id
