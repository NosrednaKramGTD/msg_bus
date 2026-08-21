"""Queue message data transfer objects.

Defines the shape of messages sent through the queue: payload (data) and
metadata (queue name, correlation id, error info, etc.).
"""

from datetime import datetime

from pydantic import BaseModel, Field


class MetaDTO(BaseModel):
    """Metadata for a queue message (queue name, correlation, errors, version)."""

    queue_name: str = Field(..., description="Name of the queue")
    correlation_id: int | None = Field(None, description="Correlation identifier")
    correlation_queue: str | None = Field(None, description="Correlation queue name")
    error_message: str | None = Field(None, description="Error message if any")
    stack_trace: str | None = Field(None, description="Trace of the error if any")
    target_id: str | None = Field(None, description="Associated target identifier, often Institution ID")
    version: str | None = Field(None, description="Version of the message")


class DataDTO(BaseModel):
    """A queue message: payload plus metadata.

    Used when enqueueing. Handlers receive this as a dict ``{"data": ..., "meta": ...}``.
    """

    data: dict = Field(..., description="Application payload (JSON-serializable dict)")
    meta: MetaDTO = Field(..., description="Message metadata")


class QueueMessage(BaseModel):
    """Dequeued message: backend id plus the application payload."""

    msg_id: int = Field(..., description="Backend message identifier")
    payload: DataDTO = Field(..., description="Application payload (data + meta)")
    read_ct: int | None = Field(None, description="Times this message has been read")
    enqueued_at: datetime | None = Field(None, description="When the message was enqueued")
    vt: datetime | None = Field(None, description="Visibility timeout expiry")
