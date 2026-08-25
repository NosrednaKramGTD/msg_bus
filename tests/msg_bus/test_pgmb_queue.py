import os
import time
from unittest import TestCase, skipIf

from dotenv import load_dotenv

from msg_bus.exceptions import DuplicateTargetError
from msg_bus.persist_pgmq import PersistPGMQ as QueueRepository
from msg_bus.queue_model_dto import DataDTO, MetaDTO

load_dotenv()


@skipIf(not os.getenv("PGMQ_DSN"), "PGMQ_DSN not set; skip live PGMQ tests")
class TestQueueRepository(TestCase):
    @classmethod
    def setUpClass(cls):
        dsn = os.getenv("PGMQ_DSN")
        cls.repo = QueueRepository(dsn=dsn)
        cls.test_queue_name = "test_queue"
        cls.repo.create_queue(cls.test_queue_name)

    @classmethod
    def tearDownClass(cls):
        cls.repo.destroy_queue(cls.test_queue_name)
        cls.repo.close()

    def test_create_list_destroy_queue(self):
        queue_name = "temp_test_queue"
        self.repo.create_queue(queue_name)
        queues = self.repo.list_queues()
        self.assertIn(queue_name, queues)

        self.repo.destroy_queue(queue_name)
        queues = self.repo.list_queues()
        self.assertNotIn(queue_name, queues)

    def test_enqueue_dequeue(self):
        data = {
            "key": "value",
        }
        meta = {
            "queue_name": self.test_queue_name,
        }
        message_data = DataDTO(data=data, meta=meta)
        message_id = self.repo.enqueue(message_data)
        self.assertIsInstance(message_id, int)

        message = self.repo.dequeue(
            queue_name=self.test_queue_name,
            options={"visibility_timeout": 10},  # seconds
        )
        self.assertIsNotNone(message)
        self.assertEqual(message.msg_id, message_id)

    def test_delete_message(self):
        data = {
            "key": "value_to_delete",
        }
        meta = {
            "queue_name": self.test_queue_name,
        }
        message_data = DataDTO(data=data, meta=meta)
        message_id = self.repo.enqueue(message_data)

        message = self.repo.dequeue(
            queue_name=self.test_queue_name,
            options={"visibility_timeout": 1},  # seconds
        )
        self.assertIsNotNone(message)
        self.assertEqual(message.msg_id, message_id)

        time.sleep(1.5)  # Wait for visibility timeout to expire

        self.repo.delete(
            queue_name=self.test_queue_name,
            id=message_id,
        )

        message_after_delete = self.repo.dequeue(
            queue_name=self.test_queue_name,
            options={"visibility_timeout": 10},  # seconds
        )
        self.assertIsNone(message_after_delete)

    def test_archive_message(self):
        data = {
            "key": "value_to_archive",
        }
        meta = {
            "queue_name": self.test_queue_name,
        }
        message_data = DataDTO(data=data, meta=meta)
        message_id = self.repo.enqueue(message_data)

        message = self.repo.dequeue(
            queue_name=self.test_queue_name,
            options={"visibility_timeout": 1},  # seconds
        )
        self.assertIsNotNone(message)
        self.assertEqual(message.msg_id, message_id)

        time.sleep(1.5)  # Wait for visibility timeout to expire

        self.repo.archive(
            queue_name=self.test_queue_name,
            id=message_id,
        )

        message_after_archive = self.repo.dequeue(
            queue_name=self.test_queue_name,
            options={"visibility_timeout": 10},  # seconds
        )
        self.assertIsNone(message_after_archive)

    def test_read_message_visibility(self):
        self.repo.purge_queue(self.test_queue_name)
        data = {
            "key": "value",
        }
        meta = {
            "queue_name": self.test_queue_name,
        }
        message_data = DataDTO(data=data, meta=meta)
        message_id = self.repo.enqueue(message_data)

        # Read the message without removing it
        message = self.repo.dequeue(
            queue_name=self.test_queue_name,
            options={"visibility_timeout": 1},  # seconds
        )

        self.assertIsNotNone(message)
        self.assertEqual(message.msg_id, message_id)

        message2 = self.repo.dequeue(
            queue_name=self.test_queue_name,
            options={"visibility_timeout": 1},  # seconds
        )

        self.assertIsNone(message2)

        time.sleep(1.5)  # Wait for visibility timeout to expire

        message3 = self.repo.dequeue(
            queue_name=self.test_queue_name,
            options={"visibility_timeout": 1},  # seconds
        )

        self.assertIsNotNone(message3)
        self.assertEqual(message3.msg_id, message_id)

    def test_enqueue_rejects_duplicate_target_until_archived(self):
        self.repo.purge_queue(self.test_queue_name)
        target_id = "dup-target-1"
        first = DataDTO(
            data={"seq": 1},
            meta=MetaDTO(queue_name=self.test_queue_name, target_id=target_id),
        )
        first_id = self.repo.enqueue(first)
        self.assertIsInstance(first_id, int)

        with self.assertRaises(DuplicateTargetError) as raised:
            self.repo.enqueue(
                DataDTO(
                    data={"seq": 2},
                    meta=MetaDTO(queue_name=self.test_queue_name, target_id=target_id),
                )
            )
        self.assertEqual(raised.exception.existing_msg_id, first_id)

        self.repo.archive(self.test_queue_name, first_id)
        third_id = self.repo.enqueue(
            DataDTO(
                data={"seq": 3},
                meta=MetaDTO(queue_name=self.test_queue_name, target_id=target_id),
            )
        )
        self.assertIsInstance(third_id, int)
        self.assertNotEqual(third_id, first_id)
        self.repo.delete(self.test_queue_name, third_id)

    def test_find_archived_event_after_archive_not_by_target(self):
        self.repo.purge_queue(self.test_queue_name)
        event_key = "workday:hire:E123:2026-08-25"
        first_id = self.repo.enqueue(
            DataDTO(
                data={"seq": 1},
                meta=MetaDTO(
                    queue_name=self.test_queue_name,
                    target_id="E123",
                    event_key=event_key,
                ),
            )
        )
        self.assertIsNone(self.repo.find_archived_event(self.test_queue_name, event_key))

        self.repo.archive(self.test_queue_name, first_id)
        self.assertEqual(self.repo.find_archived_event(self.test_queue_name, event_key), first_id)
        self.assertIsNone(
            self.repo.find_archived_event(
                self.test_queue_name,
                "workday:hire:E123:2027-01-15",
            )
        )
        self.assertIsNone(self.repo.find_archived_event(self.test_queue_name, "  "))
