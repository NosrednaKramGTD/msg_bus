"""Unit tests for the library process loop."""

from unittest import TestCase
from unittest.mock import MagicMock

from msg_bus.processor import handle_message, process_queues, validate_message
from msg_bus.queue_model_dto import DataDTO, MetaDTO, QueueMessage


def _queue_message(msg_id: int = 1, queue_name: str = "q1") -> QueueMessage:
    return QueueMessage(
        msg_id=msg_id,
        payload=DataDTO(data={"k": "v"}, meta=MetaDTO(queue_name=queue_name)),
    )


class TestValidateAndHandle(TestCase):
    def test_validate_message_calls_handler_validate_when_present(self):
        handlers = {"q1": MagicMock()}
        msg = {"data": {}, "meta": {}}
        validate_message(msg, handlers, "q1")
        handlers["q1"].validate.assert_called_once_with(msg)

    def test_validate_message_no_op_when_no_validate(self):
        class HandlerWithoutValidate:
            def handle(self, message):
                pass

        validate_message({}, {"q1": HandlerWithoutValidate()}, "q1")

    def test_handle_message_calls_validate_then_handle(self):
        handler = MagicMock()
        msg = {"data": {}}
        handle_message(msg, {"q1": handler}, "q1")
        handler.validate.assert_called_once_with(msg)
        handler.handle.assert_called_once_with(msg)

    def test_handle_message_calls_handle_without_validate(self):
        class HandlerWithoutValidate:
            def __init__(self):
                self.seen = None

            def handle(self, message):
                self.seen = message

        handler = HandlerWithoutValidate()
        msg = {"data": {"ok": True}}
        handle_message(msg, {"q1": handler}, "q1")
        self.assertEqual(handler.seen, msg)


class TestProcessQueues(TestCase):
    def test_empty_queue_breaks_without_busy_loop(self):
        repo = MagicMock()
        repo.dequeue.return_value = None
        handler = MagicMock()
        process_queues(repo, ["q1"], {"q1": handler}, max_messages=50, max_runtime=600)
        self.assertEqual(repo.dequeue.call_count, 1)
        handler.handle.assert_not_called()
        repo.archive.assert_not_called()

    def test_handle_without_validate_archives(self):
        repo = MagicMock()
        msg = _queue_message()
        repo.dequeue.side_effect = [msg, None]

        class HandlerWithoutValidate:
            def __init__(self):
                self.seen = None

            def handle(self, message):
                self.seen = message

        handler = HandlerWithoutValidate()
        process_queues(repo, ["q1"], {"q1": handler}, max_messages=10)
        self.assertEqual(handler.seen["data"], {"k": "v"})
        self.assertEqual(handler.seen["meta"]["queue_name"], "q1")
        repo.archive.assert_called_once_with("q1", 1)
        repo.delete.assert_not_called()

    def test_validate_then_handle_once(self):
        repo = MagicMock()
        msg = _queue_message()
        repo.dequeue.side_effect = [msg, None]
        handler = MagicMock()
        process_queues(repo, ["q1"], {"q1": handler}, max_messages=10)
        handler.validate.assert_called_once()
        handler.handle.assert_called_once()
        payload = handler.handle.call_args[0][0]
        self.assertEqual(payload["data"], {"k": "v"})

    def test_enqueue_error_aligned_signature(self):
        repo = MagicMock()
        msg = _queue_message(msg_id=5)
        repo.dequeue.side_effect = [msg, None]
        handler = MagicMock()
        handler.handle.side_effect = RuntimeError("boom")
        repo.enqueue_error.return_value = 12

        process_queues(repo, ["q1"], {"q1": handler}, error_visibility_timeout=9, max_messages=10)

        repo.enqueue_error.assert_called_once()
        args, kwargs = repo.enqueue_error.call_args
        self.assertEqual(args[0], "q1")
        self.assertEqual(args[1], 5)
        self.assertEqual(args[2]["data"], {"k": "v"})
        self.assertIn("boom", args[2]["meta"]["error_message"])
        self.assertIn("stack_trace", args[2]["meta"])
        self.assertEqual(kwargs["visibility_timeout"], 9)
        repo.archive.assert_not_called()

    def test_delete_messages_flag(self):
        repo = MagicMock()
        msg = _queue_message()
        repo.dequeue.side_effect = [msg, None]
        handler = MagicMock()
        process_queues(repo, ["q1"], {"q1": handler}, delete_messages=True, max_messages=10)
        repo.delete.assert_called_once_with("q1", 1)
        repo.archive.assert_not_called()

    def test_validate_only_does_not_handle_or_remove(self):
        repo = MagicMock()
        msg = _queue_message()
        repo.dequeue.side_effect = [msg, None]
        handler = MagicMock()
        process_queues(repo, ["q1"], {"q1": handler}, validate_only=True)
        handler.validate.assert_called_once()
        handler.handle.assert_not_called()
        repo.archive.assert_not_called()
        repo.delete.assert_not_called()
        repo.enqueue_error.assert_not_called()
