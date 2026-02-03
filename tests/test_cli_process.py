"""Tests for the process CLI."""

from unittest import TestCase
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from msg_bus.cli.process import (
    get_dsn,
    get_handlers,
    handle_message,
    main,
    validate_message,
)


class TestGetDsn(TestCase):
    """Tests for get_dsn helper."""

    def test_get_dsn_uses_arg_when_provided(self):
        self.assertEqual(get_dsn("postgres:///db"), "postgres:///db")

    def test_get_dsn_uses_env_when_arg_empty(self):
        with patch.dict("os.environ", {"PGMQ_DSN": "postgres://env/db"}, clear=False):
            self.assertEqual(get_dsn(None), "postgres://env/db")
            self.assertEqual(get_dsn(""), "postgres://env/db")

    def test_get_dsn_raises_when_missing(self):
        with patch.dict("os.environ", {"PGMQ_DSN": ""}, clear=False):
            with self.assertRaises(Exception) as ctx:
                get_dsn(None)
            self.assertIn("No DSN provided", str(ctx.exception))


class TestGetHandlers(TestCase):
    """Tests for get_handlers."""

    def test_get_handlers_raises_when_queue_does_not_exist(self):
        with self.assertRaises(Exception) as ctx:
            get_handlers(
                queue_names=["missing"],
                queues=["other"],
                handlers_path=["/tmp"],
            )
        self.assertIn("does not exist", str(ctx.exception))

    @patch("msg_bus.cli.process.importlib.import_module")
    def test_get_handlers_loads_handler_per_queue(self, mock_import):
        mock_module = MagicMock()
        mock_handler = MagicMock()
        mock_module.Handler.return_value = mock_handler
        mock_import.return_value = mock_module

        result = get_handlers(
            queue_names=["q1"],
            queues=["q1"],
            handlers_path=["/nonexistent"],
        )
        self.assertEqual(list(result.keys()), ["q1"])
        self.assertIs(result["q1"], mock_handler)
        mock_import.assert_called_with("handlers.q1")

    @patch("msg_bus.cli.process.importlib.import_module")
    def test_get_handlers_validate_only_raises_when_no_validate(self, mock_import):
        mock_module = MagicMock()
        mock_handler = MagicMock(spec=[])  # no validate
        del mock_handler.validate
        mock_module.Handler.return_value = mock_handler
        mock_import.return_value = mock_module

        with self.assertRaises(Exception) as ctx:
            get_handlers(
                queue_names=["q1"],
                queues=["q1"],
                handlers_path=["/tmp"],
                validate_only=True,
            )
        self.assertIn("No validator", str(ctx.exception))

    @patch("msg_bus.cli.process.importlib.import_module")
    def test_get_handlers_validate_only_succeeds_with_validate(self, mock_import):
        mock_module = MagicMock()
        mock_handler = MagicMock()
        mock_handler.validate = MagicMock()
        mock_module.Handler.return_value = mock_handler
        mock_import.return_value = mock_module

        result = get_handlers(
            queue_names=["q1"],
            queues=["q1"],
            handlers_path=["/tmp"],
            validate_only=True,
        )
        self.assertIn("q1", result)
        self.assertTrue(hasattr(result["q1"], "validate"))


class TestValidateMessage(TestCase):
    """Tests for validate_message helper."""

    def test_validate_message_calls_handler_validate_when_present(self):
        handlers = {"q1": MagicMock()}
        handlers["q1"].validate = MagicMock()
        msg = {"data": {}, "meta": {}}
        validate_message(msg, handlers, "q1")
        handlers["q1"].validate.assert_called_once_with(msg)

    def test_validate_message_no_op_when_no_validate(self):
        class HandlerWithoutValidate:
            def handle(self, message):
                pass

        handler = HandlerWithoutValidate()
        handlers = {"q1": handler}
        msg = {}
        validate_message(msg, handlers, "q1")
        # No exception; validate is not called because handler has no validate


class TestHandleMessage(TestCase):
    """Tests for handle_message helper."""

    def test_handle_message_calls_validate_then_handle(self):
        handler = MagicMock()
        handler.validate = MagicMock()
        handler.handle = MagicMock()
        handlers = {"q1": handler}
        msg = {"data": {}}
        handle_message(msg, handlers, "q1")
        handler.validate.assert_called_once_with(msg)
        handler.handle.assert_called_once_with(msg)


class TestProcessCLI(TestCase):
    """Tests for the process CLI command."""

    def setUp(self):
        self.runner = CliRunner()

    def test_process_requires_queue_names(self):
        result = self.runner.invoke(
            main,
            ["--handlers-path", "/tmp"],
            env={"PGMQ_DSN": "postgres:///db"},
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing option", result.output)

    def test_process_requires_handlers_path(self):
        result = self.runner.invoke(
            main,
            ["--queue-names", "q1"],
            env={"PGMQ_DSN": "postgres:///db"},
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing option", result.output)

    @patch("msg_bus.cli.process.os.getenv", return_value=None)
    def test_process_fails_without_dsn_and_env(self, mock_getenv):
        result = self.runner.invoke(
            main,
            ["--queue-names", "q1", "--handlers-path", "/tmp"],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No DSN provided", result.output + result.stderr)

    @patch("msg_bus.cli.process.get_handlers")
    @patch("msg_bus.cli.process.QueueRepository")
    def test_process_invokes_repo_and_handlers_path(self, mock_repo_class, mock_get_handlers):
        mock_repo = MagicMock()
        mock_repo.list_queues.return_value = ["my_queue"]
        mock_repo.dequeue.return_value = None
        mock_repo_class.return_value = mock_repo

        mock_handler = MagicMock()
        mock_handler.validate = MagicMock()
        mock_handler.handle = MagicMock()
        mock_get_handlers.return_value = {"my_queue": mock_handler}

        result = self.runner.invoke(
            main,
            [
                "--queue-names",
                "my_queue",
                "--handlers-path",
                "/tmp",
                "--dsn",
                "postgres:///db",
                "--max-messages",
                "1",
            ],
        )
        self.assertEqual(result.exit_code, 0)
        mock_repo_class.assert_called_once_with(dsn="postgres:///db")
        mock_get_handlers.assert_called_once()
        pos_args = mock_get_handlers.call_args[0]
        call_kw = mock_get_handlers.call_args[1]
        self.assertEqual(pos_args[0], ["my_queue"])
        self.assertEqual(pos_args[1], ["my_queue"])
        self.assertIn("handlers_path", call_kw)
        mock_repo.close.assert_called_once()
