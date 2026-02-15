"""Tests for the status CLI."""

from unittest import TestCase
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from msg_bus.cli.status import main, queue_exists


class TestStatusQueueExists(TestCase):
    """Tests for queue_exists helper in status module."""

    def test_queue_exists_true(self):
        repo = MagicMock()
        repo.list_queues.return_value = ["q1", "q2"]
        self.assertTrue(queue_exists(repo, "q1"))

    def test_queue_exists_false(self):
        repo = MagicMock()
        repo.list_queues.return_value = ["q1"]
        self.assertFalse(queue_exists(repo, "q2"))


class TestStatusCLI(TestCase):
    """Tests for the status CLI command."""

    def setUp(self):
        self.runner = CliRunner()

    def test_status_requires_queue_name(self):
        result = self.runner.invoke(main, [])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Missing option", result.output)

    @patch("msg_bus.cli.status.os.getenv", return_value=None)
    def test_status_fails_without_dsn_and_env(self, mock_getenv):
        result = self.runner.invoke(
            main,
            ["--queue-name", "q1"],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("No DSN provided", result.output + result.stderr)

    def test_status_fails_when_queue_does_not_exist(self):
        with patch("msg_bus.cli.status.QueueRepository") as mock_repo_class:
            mock_repo = MagicMock()
            mock_repo.list_queues.return_value = ["other_queue"]
            mock_repo_class.return_value = mock_repo

            result = self.runner.invoke(
                main,
                ["--queue-name", "missing_queue", "--dsn", "postgres:///db"],
            )
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("does not exist", result.output)
            mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.status.QueueRepository")
    def test_status_prints_metrics(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.list_queues.return_value = ["my_queue"]
        mock_repo.metrics.return_value = {
            "queue_name": "my_queue",
            "queue_length": 5,
            "newest_msg_age_sec": 1,
            "oldest_msg_age_sec": 10,
        }
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "my_queue", "--dsn", "postgres:///db"],
        )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Queue status", result.output)
        mock_repo.metrics.assert_called_once_with("my_queue")
        mock_repo.close.assert_called_once()

    @patch("msg_bus.cli.status.QueueRepository")
    def test_status_uses_pgmq_dsn_env_when_dsn_not_provided(self, mock_repo_class):
        mock_repo = MagicMock()
        mock_repo.list_queues.return_value = ["q1"]
        mock_repo.metrics.return_value = {}
        mock_repo_class.return_value = mock_repo

        result = self.runner.invoke(
            main,
            ["--queue-name", "q1"],
            env={"PGMQ_DSN": "postgres://localhost/db"},
        )
        self.assertEqual(result.exit_code, 0)
        mock_repo_class.assert_called_once_with(dsn="postgres://localhost/db")
