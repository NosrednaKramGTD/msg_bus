"""msg_bus: message queue utilities with PGMQ-backed persistence and per-queue handlers."""

from msg_bus.exceptions import DuplicateTargetError
from msg_bus.handlers.base import BaseHandler
from msg_bus.persist_base import PersistBase
from msg_bus.persist_pgmq import PersistPGMQ
from msg_bus.processor import process_queues
from msg_bus.queue_model_dto import ActionType, DataDTO, MetaDTO, QueueMessage

__all__ = [
    "ActionType",
    "BaseHandler",
    "DataDTO",
    "DuplicateTargetError",
    "MetaDTO",
    "PersistBase",
    "PersistPGMQ",
    "QueueMessage",
    "process_queues",
]
