"""Tests for public package exports."""

from unittest import TestCase

import msg_bus
from msg_bus import (
    BaseHandler,
    DataDTO,
    DuplicateTargetError,
    MetaDTO,
    PersistBase,
    PersistPGMQ,
    QueueMessage,
    process_queues,
)


class TestPublicApi(TestCase):
    def test_all_exports_present(self):
        for name in (
            "BaseHandler",
            "DataDTO",
            "DuplicateTargetError",
            "MetaDTO",
            "PersistBase",
            "PersistPGMQ",
            "QueueMessage",
            "process_queues",
        ):
            self.assertIn(name, msg_bus.__all__)
            self.assertTrue(hasattr(msg_bus, name))

    def test_exports_are_expected_types(self):
        self.assertTrue(issubclass(BaseHandler, object))
        self.assertTrue(issubclass(DuplicateTargetError, Exception))
        self.assertTrue(issubclass(PersistPGMQ, PersistBase))
        self.assertTrue(callable(process_queues))
        dto = DataDTO(data={}, meta=MetaDTO(queue_name="q", source_system="workday"))
        msg = QueueMessage(msg_id=1, payload=dto)
        self.assertEqual(msg.payload.meta.queue_name, "q")
        self.assertEqual(msg.payload.meta.source_system, "workday")
