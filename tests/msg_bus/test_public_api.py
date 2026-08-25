"""Tests for public package exports."""

from unittest import TestCase

import msg_bus
from msg_bus import (
    ActionType,
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
            "ActionType",
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
        dto = DataDTO(
            data={"preferred_delivery_method": "EMAIL"},
            meta=MetaDTO(
                queue_name="account_create",
                source_system="workday",
                action_type=ActionType.ADD,
                business_reason="hire",
                associated_period="2026FA",
            ),
        )
        msg = QueueMessage(msg_id=1, payload=dto)
        self.assertEqual(msg.payload.meta.queue_name, "account_create")
        self.assertEqual(msg.payload.meta.source_system, "workday")
        self.assertEqual(msg.payload.meta.action_type, "add")
        self.assertEqual(msg.payload.meta.business_reason, "hire")
        self.assertEqual(msg.payload.meta.associated_period, "2026FA")
        self.assertEqual(msg.payload.data["preferred_delivery_method"], "EMAIL")
        unlocked = MetaDTO(queue_name="account_update", action_type="unlock")
        self.assertEqual(unlocked.action_type, "unlock")
        custom_reason = MetaDTO(queue_name="communication", business_reason="rehire")
        self.assertEqual(custom_reason.business_reason, "rehire")
