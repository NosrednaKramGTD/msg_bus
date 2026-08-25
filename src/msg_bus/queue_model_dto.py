"""Queue message data transfer objects.

Defines the shape of messages sent through the queue: payload (data) and
metadata (queue name, correlation id, error info, etc.).
"""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class ActionType(StrEnum):
    """Canonical values for ``MetaDTO.action_type``. Other strings are also allowed."""

    ADD = "add"
    UPDATE = "update"
    REMOVE = "remove"
    COMMUNICATION = "communication"


class MetaDTO(BaseModel):
    """Metadata for a queue message (queue name, reason, period, correlation, source, errors, version)."""

    queue_name: str = Field(
        ...,
        description="How the message is processed (handler/work stream), e.g. account_create, communication",
    )
    correlation_id: int | None = Field(None, description="Correlation identifier")
    correlation_queue: str | None = Field(None, description="Correlation queue name")
    error_message: str | None = Field(None, description="Error message if any")
    stack_trace: str | None = Field(None, description="Trace of the error if any")
    target_id: str | None = Field(None, description="Associated target identifier, often Institution ID")
    source_system: str | None = Field(None, description="System that produced the message")
    action_type: str | None = Field(
        None,
        description="Kind of change: add, update, remove, lock, or another producer-defined value",
    )
    business_reason: str | None = Field(
        None,
        description="Producer-defined reason the work was requested; the bus does not constrain values",
    )
    associated_period: str | None = Field(
        None,
        description="Optional academic period associated with the message (e.g. 2026FA)",
    )
    version: str | None = Field(None, description="Version of the message")


class DataDTO(BaseModel):
    """A queue message: payload plus metadata.

    Used when enqueueing. Handlers receive this as a dict ``{"data": ..., "meta": ...}``.
    Payload field names are by queue convention (for example ``preferred_delivery_method``).
    """

    data: dict = Field(
        ...,
        description="Application payload (JSON-serializable dict). Field names are by queue convention, e.g. preferred_delivery_method",
    )
    meta: MetaDTO = Field(..., description="Message metadata")


class QueueMessage(BaseModel):
    """Dequeued message: backend id plus the application payload.

    ``payload`` is set when the stored JSON matches DataDTO. Otherwise it is
    None and ``raw_payload`` holds the original value so callers can still
    archive, delete, or dead-letter by ``msg_id``.
    """

    msg_id: int = Field(..., description="Backend message identifier")
    payload: DataDTO | None = Field(None, description="Application payload (data + meta) when valid")
    raw_payload: Any = Field(None, description="Original stored JSON when payload validation failed")
    read_ct: int | None = Field(None, description="Times this message has been read")
    enqueued_at: datetime | None = Field(None, description="When the message was enqueued")
    vt: datetime | None = Field(None, description="Visibility timeout expiry")
