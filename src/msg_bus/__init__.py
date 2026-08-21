"""msg_bus: message queue utilities with PGMQ-backed persistence and per-queue handlers."""

from msg_bus.handlers.base import BaseHandler
from msg_bus.persist_base import PersistBase
from msg_bus.persist_pgmq import PersistPGMQ
from msg_bus.processor import process_queues
from msg_bus.queue_model_dto import DataDTO, MetaDTO, QueueMessage

__all__ = [
    "BaseHandler",
    "DataDTO",
    "MetaDTO",
    "PersistBase",
    "PersistPGMQ",
    "QueueMessage",
    "process_queues",
]
