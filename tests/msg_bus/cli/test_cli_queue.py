"""Tests for the queue CLI."""

from unittest import TestCase
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from msg_bus.cli.queue import main, queue_exists


class TestQueueQueueExists(TestCase):
    """Tests for queue_exists helper in queue module."""

    def test_queue_exists_true(self):
        repo = MagicMock()
        repo.list_queues.return_value = ["q1", "q2"]
        self.assertTrue(queue_exists(repo, "q1"))

    def test_queue_exists_false(self):
        repo = MagicMock()
        repo.list_queues.return_value = ["q1"]
        self.assertFalse(queue_exists(repo, "q2"))


class TestQueueCLI(TestCase):
    """Tests for the queue CLI command."""

    def setUp(self):
        self.runner = CliRunner()

    def test_requires_queue_name(self):
        result = self.runner.invoke(main, ["--action", "status"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing option", result.output)

    def test_requires_action(self):
        result = self.runner.invoke(main, ["--queue-name", "q1", "--dsn", "postgres:///db"])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing option", result.output)

    @patch("msg_bus.cli.queue.os.getenv", return_value=None)
    def test_fails_without_dsn_and_env(self, mock_getenv):
        result = self.runner.invoke(
            main,
            ["--queue-name", "q1", "--action", "status"],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No DSN provided", result.output + result.stderr)

    @patch("msg_bus.cli.queue.QueueRepository")
    def test_action_create_success(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "my_queue", "--dsn", "postgres:///db", "--action", "create"],
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Queue my_queue created", result.output)
        mock_repo.create_queue.assert_called_once_with("my_queue")
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.queue.QueueRepository")
    def test_action_status_prints_metrics(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.metrics.return_value = {
            "queue_name": "my_queue",
            "queue_length": 5,
            "newest_msg_age_sec": 1,
            "oldest_msg_age_sec": 10,
        }
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "my_queue", "--dsn", "postgres:///db", "--action", "status"],
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Queue my_queue status", result.output)
        mock_repo.metrics.assert_called_once_with("my_queue")
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.queue.QueueRepository")
    def test_action_destroy_success(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "my_queue", "--dsn", "postgres:///db", "--action", "destroy"],
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Queue my_queue destroyed", result.output)
        mock_repo.destroy_queue.assert_called_once_with("my_queue")
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.queue.QueueRepository")
    def test_action_purge_success(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.purge_queue.return_value = 42
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "my_queue", "--dsn", "postgres:///db", "--action", "purge"],
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Queue my_queue purged", result.output)
        mock_repo.purge_queue.assert_called_once_with("my_queue")
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.queue.QueueRepository")
    def test_invalid_action_raises_click_exception(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "my_queue", "--dsn", "postgres:///db", "--action", "invalid"],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Invalid action", result.output + result.stderr)
        self.assertIn("create, status, destroy, purge", result.output + result.stderr)
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.queue.QueueRepository")
    def test_uses_pgmq_dsn_env_when_dsn_not_provided(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.metrics.return_value = {}
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "q1", "--action", "status"],
            env={"PGMQ_DSN": "postgres://localhost/db"},
        )
        self.assertEqual(result.exit_code, 0)
        mock_repo_class.assert_called_once_with(dsn="postgres://localhost/db")
