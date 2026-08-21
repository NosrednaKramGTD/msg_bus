"""Unit tests for PersistPGMQ (PGMQueue mocked)."""

from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

from pgmq.messages import QueueMetrics

from msg_bus.persist_pgmq import PersistPGMQ, _flag_true
from msg_bus.queue_model_dto import QueueMessage


class TestFlagTrue(TestCase):
    def test_bool_and_strings(self):
        self.assertTrue(_flag_true(True))
        self.assertTrue(_flag_true("true"))
        self.assertTrue(_flag_true("YES"))
        self.assertTrue(_flag_true("1"))
        self.assertFalse(_flag_true(False))
        self.assertFalse(_flag_true("false"))
        self.assertFalse(_flag_true(None))


class TestPersistPGMQ(TestCase):
    def setUp(self):
        self.pgmq_patcher = patch("msg_bus.persist_pgmq.PGMQueue")
        mock_cls = self.pgmq_patcher.start()
        self.raw = MagicMock()
        mock_cls.return_value = self.raw
        self.repo = PersistPGMQ(dsn="postgresql://user:pass@localhost:5432/db")

    def tearDown(self):
        self.pgmq_patcher.stop()

    def test_init_passes_parsed_dsn(self):
        with patch("msg_bus.persist_pgmq.PGMQueue") as mock_cls:
            PersistPGMQ(dsn="postgresql://alice:s3cret@dbhost:5555/pgmq")
        mock_cls.assert_called_once()
        kwargs = mock_cls.call_args.kwargs
        self.assertEqual(kwargs["host"], "dbhost")
        self.assertEqual(kwargs["port"], 5555)
        self.assertEqual(kwargs["database"], "pgmq")
        self.assertEqual(kwargs["username"], "alice")
        self.assertEqual(kwargs["password"], "s3cret")

    def test_dequeue_options_none_uses_default_vt(self):
        self.raw.read.return_value = None
        result = self.repo.dequeue("q1", options=None)
        self.assertIsNone(result)
        self.raw.read.assert_called_once_with(queue="q1", vt=300)

    def test_dequeue_maps_to_queue_message(self):
        raw_msg = MagicMock()
        raw_msg.msg_id = 7
        raw_msg.read_ct = 1
        raw_msg.enqueued_at = None
        raw_msg.vt = None
        raw_msg.message = {"data": {"a": 1}, "meta": {"queue_name": "q1"}}
        self.raw.read.return_value = raw_msg

        result = self.repo.dequeue("q1", options={"visibility_timeout": 10})
        self.assertIsInstance(result, QueueMessage)
        self.assertEqual(result.msg_id, 7)
        self.assertEqual(result.payload.data, {"a": 1})
        self.assertEqual(result.payload.meta.queue_name, "q1")
        self.raw.read.assert_called_once_with(queue="q1", vt=10)

    def test_create_partitioned_queue_kwargs(self):
        self.repo.create_queue("q1", options={"partition": True, "interval": 10, "retention": 20})
        self.raw.create_partitioned_queue.assert_called_once_with(
            "q1",
            partition_interval=10,
            retention_interval=20,
        )
        self.raw.create_queue.assert_not_called()

    def test_create_partitioned_queue_string_true(self):
        self.repo.create_queue("q1", options={"partition": "true"})
        self.raw.create_partitioned_queue.assert_called_once()
        kwargs = self.raw.create_partitioned_queue.call_args.kwargs
        self.assertIn("partition_interval", kwargs)

    def test_create_plain_queue(self):
        self.repo.create_queue("q1")
        self.raw.create_queue.assert_called_once_with("q1")
        self.raw.create_partitioned_queue.assert_not_called()

    def test_enqueue_error_signature(self):
        self.raw.send.return_value = 99
        payload = {"data": {}, "meta": {"queue_name": "q1", "error_message": "boom"}}
        conn = MagicMock()
        result = self.repo.enqueue_error("q1", 5, payload, visibility_timeout=10, conn=conn)
        self.assertEqual(result, 99)
        self.raw.send.assert_called_once()
        send_kw = self.raw.send.call_args.kwargs
        self.assertEqual(send_kw["queue"], "q1")
        self.assertEqual(send_kw["message"], payload)
        self.raw.delete.assert_called_once_with(queue="q1", msg_id=5, conn=conn)
        self.raw.set_vt.assert_called_once_with(queue="q1", msg_id=99, vt=10, conn=conn)

    def test_metrics_as_dict(self):
        self.raw.metrics.return_value = QueueMetrics(
            queue_name="q1",
            queue_length=1,
            newest_msg_age_sec=1,
            oldest_msg_age_sec=2,
            total_messages=3,
            scrape_time=datetime(2026, 1, 1),
        )
        result = self.repo.metrics("q1")
        self.assertIsInstance(result, dict)
        self.assertEqual(result["queue_name"], "q1")
        self.assertEqual(result["queue_length"], 1)
