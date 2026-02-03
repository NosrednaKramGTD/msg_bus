"""Tests for the enqueue CLI."""

import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from msg_bus.cli.enqueue import main, queue_exists


class TestQueueExists(TestCase):
    """Tests for queue_exists helper."""

    def test_queue_exists_true(self):
        repo = MagicMock()
        repo.list_queues.return_value = ["q1", "q2"]
        self.assertTrue(queue_exists(repo, "q1"))
        self.assertTrue(queue_exists(repo, "q2"))

    def test_queue_exists_false(self):
        repo = MagicMock()
        repo.list_queues.return_value = ["q1"]
        self.assertFalse(queue_exists(repo, "q2"))


class TestEnqueueCLI(TestCase):
    """Tests for the enqueue CLI command."""

    def setUp(self):
        self.runner = CliRunner()

    def test_enqueue_requires_queue_name(self):
        result = self.runner.invoke(main, ["--message", "{}"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing option", result.output)

    def test_enqueue_requires_message(self):
        result = self.runner.invoke(main, ["--queue-name", "q1"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing option", result.output)

    @patch("msg_bus.cli.enqueue.os.getenv", return_value=None)
    def test_enqueue_fails_without_dsn_and_env(self, mock_getenv):
        result = self.runner.invoke(
            main,
            ["--queue-name", "q1", "--message", "{}"],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No DSN provided", result.output + result.stderr)

    @patch("msg_bus.cli.enqueue.QueueRepository")
    def test_enqueue_invalid_json(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.list_queues.return_value = ["q1"]
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "q1", "--message", "not json", "--dsn", "postgres:///db"],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Invalid JSON", result.output)
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.enqueue.QueueRepository")
    def test_enqueue_creates_queue_if_missing_and_enqueues(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.list_queues.return_value = []
        mock_repo.enqueue.return_value = 42
        mock_repo_class.return_value = mock_repo

        msg = {"key": "value"}
        result = self.runner.invoke(
            main,
            [
                "--queue-name",
                "my_queue",
                "--message",
                json.dumps(msg),
                "--dsn",
                "postgres:///db",
            ],
        )
        self.assertEqual(result.exit_code, 0)
        mock_repo.create_queue.assert_called_once_with("my_queue")
        mock_repo.enqueue.assert_called_once()
        call_dto = mock_repo.enqueue.call_args[0][0]
        self.assertEqual(call_dto.data, msg)
        self.assertEqual(call_dto.meta.queue_name, "my_queue")
        self.assertIn("42", result.output)
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.enqueue.QueueRepository")
    def test_enqueue_uses_existing_queue(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.list_queues.return_value = ["my_queue"]
        mock_repo.enqueue.return_value = 1
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            [
                "--queue-name",
                "my_queue",
                "--message",
                "{}",
                "--dsn",
                "postgres:///db",
            ],
        )
        self.assertEqual(result.exit_code, 0)
        mock_repo.create_queue.assert_not_called()
        mock_repo.enqueue.assert_called_once()
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.enqueue.QueueRepository")
    def test_enqueue_uses_pgmq_dsn_env_when_dsn_not_provided(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.list_queues.return_value = ["q1"]
        mock_repo.enqueue.return_value = 1
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "q1", "--message", "{}"],
            env={"PGMQ_DSN": "postgres://user:pass@host/db"},
        )
        self.assertEqual(result.exit_code, 0)
        mock_repo_class.assert_called_once_with(dsn="postgres://user:pass@host/db")

    @patch("msg_bus.cli.enqueue.QueueRepository")
    def test_enqueue_create_queue_error_raises_click_exception(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.list_queues.return_value = []
        mock_repo.create_queue.side_effect = RuntimeError("db error")
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "q1", "--message", "{}", "--dsn", "postgres:///db"],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Error creating queue", result.output)
        mock_repo.close.assert_called_once()
