"""PostgreSQL-backed queue persistence using PGMQ.

Uses the pgmq library to store queues and messages in PostgreSQL with
visibility timeouts, archiving, and metrics.
"""

import logging
import os
from dataclasses import asdict, is_dataclass
from typing import Any

from pgmq import PGMQueue
from pgmq.decorators import transaction
from psycopg import sql
from pydantic import ValidationError

from msg_bus.dsn import parse_pgmq_dsn
from msg_bus.exceptions import DuplicateTargetError
from msg_bus.persist_base import PersistBase
from msg_bus.queue_model_dto import DataDTO, QueueMessage


def _normalized_optional_str(value: str | None) -> str | None:
    """Return a stripped string, or None when missing/blank."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _flag_true(value: object) -> bool:
    """Return True for bool True or common truthy strings."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return False


def _metrics_as_dict(metrics: object) -> dict[str, Any]:
    """Normalize pgmq metrics (dataclass or mapping) to a plain dict."""
    if is_dataclass(metrics) and not isinstance(metrics, type):
        return asdict(metrics)
    if isinstance(metrics, dict):
        return metrics
    return dict(vars(metrics))


class PersistPGMQ(PersistBase):
    """Queue persistence implementation using PGMQ (PostgreSQL Message Queue).

    Connects via a Postgres DSN and delegates to PGMQueue for create, send,
    read, archive, delete, and metrics. Supports partitioned queues via
    create_queue options.
    """

    def __init__(self, dsn: str | None = None) -> None:
        """Connect to PostgreSQL using the given DSN or ``PGMQ_DSN``."""
        raw = dsn or os.getenv("PGMQ_DSN")
        parts = parse_pgmq_dsn(str(raw) if raw else "")

        self.queue = PGMQueue(
            host=parts["host"],
            port=parts["port"],
            database=parts["database"],
            username=parts["username"],
            password=parts["password"],
            verbose=False,
            log_filename="pgmq.log",
        )
        # Add logger attribute required by @transaction decorator
        self.logger = logging.getLogger(__name__)

    @property
    def pool(self):
        """Expose the queue's connection pool for transaction decorator."""
        return self.queue.pool

    @transaction
    def enqueue(self, message: DataDTO, conn=None) -> int:
        """Append the message to the queue named in message.meta.queue_name.

        Returns the new message ID. Raises DuplicateTargetError when a pending
        or in-flight message already exists for the same queue and target_id.
        """
        payload = message.model_dump()
        queue_name = message.meta.queue_name
        target_id = _normalized_optional_str(message.meta.target_id)
        if target_id:
            existing_msg_id = self._find_pending_target(queue_name, target_id, conn=conn)
            if existing_msg_id is not None:
                raise DuplicateTargetError(queue_name, target_id, existing_msg_id)
        return self.queue.send(
            queue=queue_name,
            message=payload,
            conn=conn,
        )

    @transaction
    def find_archived_event(self, queue_name: str, event_key: str, conn=None) -> int | None:
        """Return the newest archived msg_id for this event_key, or None.

        Blank event_key is not queried. Pending/in-flight rows are not searched.
        """
        key = _normalized_optional_str(event_key)
        if not key:
            return None
        if conn is None:
            raise TypeError("archived event lookup requires a database connection")
        self.queue.validate_queue_name(queue_name, conn=conn)
        query = sql.SQL(
            "SELECT msg_id FROM {}.{} WHERE message->'meta'->>'event_key' = %s "
            "ORDER BY msg_id DESC LIMIT 1"
        ).format(
            sql.Identifier("pgmq"),
            sql.Identifier(f"a_{queue_name}"),
        )
        rows = conn.execute(query, [key]).fetchall()
        if not rows:
            return None
        return rows[0][0]

    def _find_pending_target(self, queue_name: str, target_id: str, conn=None) -> int | None:
        """Return the oldest pending/in-flight msg_id for this target, if any."""
        if conn is None:
            raise TypeError("duplicate target check requires a database connection")
        self.queue.validate_queue_name(queue_name, conn=conn)
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s));",
            [queue_name, target_id],
        )
        query = sql.SQL(
            "SELECT msg_id FROM {}.{} WHERE message->'meta'->>'target_id' = %s ORDER BY msg_id LIMIT 1"
        ).format(
            sql.Identifier("pgmq"),
            sql.Identifier(f"q_{queue_name}"),
        )
        rows = conn.execute(query, [target_id]).fetchall()
        if not rows:
            return None
        return rows[0][0]

    def dequeue(self, queue_name: str, options: dict[str, Any] | None = None) -> QueueMessage | None:
        """Read one message from the queue with the given visibility timeout (seconds).

        Invalid stored JSON does not raise: ``payload`` is None and ``raw_payload``
        holds the original value so the processor can dead-letter by ``msg_id``.
        """
        options = options or {}
        visibility_timeout = options.get("visibility_timeout", 300)
        message = self.queue.read(
            queue=queue_name,
            vt=visibility_timeout,
        )
        if message is None:
            return None
        raw = message.message
        try:
            payload = DataDTO.model_validate(raw)
            raw_payload = None
        except ValidationError:
            payload = None
            raw_payload = raw
        return QueueMessage(
            msg_id=message.msg_id,
            payload=payload,
            raw_payload=raw_payload,
            read_ct=getattr(message, "read_ct", None),
            enqueued_at=getattr(message, "enqueued_at", None),
            vt=getattr(message, "vt", None),
        )

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

    def create_queue(self, queue_name: str, options: dict[str, Any] | None = None) -> None:
        """Create a new queue; options may enable partitioning (interval, retention)."""
        options = options or {}
        if _flag_true(options.get("partition", False)):
            self.queue.create_partitioned_queue(
                queue_name,
                partition_interval=int(options.get("interval", 1000)),
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

    def metrics(self, queue_name: str) -> dict[str, Any]:
        """Get metrics for the specified queue."""
        return _metrics_as_dict(self.queue.metrics(queue_name))

    def close(self) -> None:
        """Close the connection pool; call when done to avoid shutdown warnings."""
        if hasattr(self.queue, "pool") and self.queue.pool:
            self.queue.pool.close()

    @transaction
    def enqueue_error(
        self,
        queue_name: str,
        message_id: int,
        payload: dict[str, Any],
        visibility_timeout: int = 5,
        conn=None,
    ) -> int:
        """Re-enqueue the message (with error metadata), delete the original, set VT.

        Used as a dead-letter path: the same message is re-sent so it can be
        retried later with a longer visibility timeout.
        """
        error_message_id = self.queue.send(
            queue=queue_name,
            message=payload,
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
