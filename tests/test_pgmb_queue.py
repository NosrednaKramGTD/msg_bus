import time
from unittest import TestCase

from icecream import ic

from config import get_settings
from msg_bus.persist_pgmq import PersistPGMQ as QueueRepository
from msg_bus.queue_model_dto import DataDTO

settings = get_settings()

print(settings.pgmq_dsn)


class TestQueueRepository(TestCase):
    @classmethod
    def setUpClass(cls):
        ic("In setUpClass")
        cls.repo = QueueRepository(dsn=settings.pgmq_dsn)
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

        ic(message)

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
