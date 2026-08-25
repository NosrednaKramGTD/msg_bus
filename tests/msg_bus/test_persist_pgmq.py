"""Unit tests for PersistPGMQ (PGMQueue mocked)."""

from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

from pgmq.messages import QueueMetrics

from msg_bus.exceptions import DuplicateTargetError
from msg_bus.persist_pgmq import PersistPGMQ, _flag_true
from msg_bus.queue_model_dto import DataDTO, MetaDTO, QueueMessage


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

    def test_dequeue_invalid_payload_does_not_raise(self):
        raw_msg = MagicMock()
        raw_msg.msg_id = 3
        raw_msg.read_ct = 2
        raw_msg.enqueued_at = None
        raw_msg.vt = None
        raw_msg.message = {"not": "a DataDTO"}
        self.raw.read.return_value = raw_msg

        result = self.repo.dequeue("q1")
        self.assertIsInstance(result, QueueMessage)
        self.assertEqual(result.msg_id, 3)
        self.assertIsNone(result.payload)
        self.assertEqual(result.raw_payload, {"not": "a DataDTO"})

    def test_dequeue_non_object_payload_does_not_raise(self):
        raw_msg = MagicMock()
        raw_msg.msg_id = 4
        raw_msg.read_ct = None
        raw_msg.enqueued_at = None
        raw_msg.vt = None
        raw_msg.message = "oops"
        self.raw.read.return_value = raw_msg

        result = self.repo.dequeue("q1")
        self.assertEqual(result.msg_id, 4)
        self.assertIsNone(result.payload)
        self.assertEqual(result.raw_payload, "oops")

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

    def test_enqueue_without_target_id_sends_only(self):
        self.raw.send.return_value = 5
        conn = MagicMock()
        result = self.repo.enqueue(
            DataDTO(data={"a": 1}, meta=MetaDTO(queue_name="q1")),
            conn=conn,
        )
        self.assertEqual(result, 5)
        self.raw.send.assert_called_once()
        send_kw = self.raw.send.call_args.kwargs
        self.assertEqual(send_kw["queue"], "q1")
        self.assertEqual(send_kw["conn"], conn)
        self.raw.validate_queue_name.assert_not_called()
        conn.execute.assert_not_called()

    def test_enqueue_blank_target_id_skips_dedupe(self):
        self.raw.send.return_value = 6
        conn = MagicMock()
        result = self.repo.enqueue(
            DataDTO(data={}, meta=MetaDTO(queue_name="q1", target_id="  ")),
            conn=conn,
        )
        self.assertEqual(result, 6)
        self.raw.send.assert_called_once()
        self.raw.validate_queue_name.assert_not_called()
        conn.execute.assert_not_called()

    def test_enqueue_rejects_duplicate_target_id(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [(42,)]
        with self.assertRaises(DuplicateTargetError) as raised:
            self.repo.enqueue(
                DataDTO(data={}, meta=MetaDTO(queue_name="account_create", target_id="E123")),
                conn=conn,
            )
        err = raised.exception
        self.assertEqual(err.queue_name, "account_create")
        self.assertEqual(err.target_id, "E123")
        self.assertEqual(err.existing_msg_id, 42)
        self.assertIn("42", str(err))
        self.raw.send.assert_not_called()
        self.raw.validate_queue_name.assert_called_once_with("account_create", conn=conn)

    def test_enqueue_sends_when_target_id_not_pending(self):
        self.raw.send.return_value = 7
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        result = self.repo.enqueue(
            DataDTO(data={"a": 1}, meta=MetaDTO(queue_name="q1", target_id="E123")),
            conn=conn,
        )
        self.assertEqual(result, 7)
        self.raw.send.assert_called_once()
        send_kw = self.raw.send.call_args.kwargs
        self.assertEqual(send_kw["queue"], "q1")
        self.assertEqual(send_kw["conn"], conn)
        self.raw.validate_queue_name.assert_called_once_with("q1", conn=conn)
        self.assertEqual(conn.execute.call_count, 2)

    def test_find_archived_event_blank_skips_query(self):
        conn = MagicMock()
        self.assertIsNone(self.repo.find_archived_event("q1", "  ", conn=conn))
        self.assertIsNone(self.repo.find_archived_event("q1", "", conn=conn))
        self.raw.validate_queue_name.assert_not_called()
        conn.execute.assert_not_called()

    def test_find_archived_event_returns_newest_msg_id(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [(99,)]
        result = self.repo.find_archived_event(
            "account_create",
            "workday:hire:E123:2026-08-25",
            conn=conn,
        )
        self.assertEqual(result, 99)
        self.raw.validate_queue_name.assert_called_once_with("account_create", conn=conn)
        conn.execute.assert_called_once()
        self.assertEqual(conn.execute.call_args.args[1], ["workday:hire:E123:2026-08-25"])

    def test_find_archived_event_returns_none_when_missing(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        result = self.repo.find_archived_event("q1", "workday:hire:E123:2026-08-25", conn=conn)
        self.assertIsNone(result)
        self.raw.validate_queue_name.assert_called_once_with("q1", conn=conn)

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
