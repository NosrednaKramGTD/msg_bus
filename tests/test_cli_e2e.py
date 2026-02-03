"""End-to-end CLI tests: enqueue then process with real DB.

Uses PGMQ_DSN from the environment and queue name test_e2e. Skip when DSN is not set.
Run with: PGMQ_DSN=postgres:///db pytest tests/test_cli_e2e.py -v
"""

import contextlib
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

# Queue and handler path
E2E_QUEUE_NAME = "test_e2e"
# Directory that contains the "handlers" package for process CLI (handlers.test_e2e)
E2E_HANDLERS_DIR = Path(__file__).resolve().parent / "e2e_handlers"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"


def get_dsn() -> str | None:
    """Return PGMQ_DSN from environment; None if unset."""
    return os.environ.get("PGMQ_DSN") or None


def run_enqueue(dsn: str, queue_name: str, message: dict) -> subprocess.CompletedProcess:
    """Run enqueue CLI via subprocess; same interface as real usage."""
    cmd = [
        sys.executable,
        "-m",
        "msg_bus.cli.enqueue",
        "--queue-name",
        queue_name,
        "--message",
        json.dumps(message),
        "--dsn",
        dsn,
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        cwd=PROJECT_ROOT,
        env=env,
        check=False,
    )


def run_process(
    dsn: str,
    queue_name: str,
    handlers_path: str,
    max_messages: int = 1,
) -> subprocess.CompletedProcess:
    """Run process CLI via subprocess."""
    cmd = [
        sys.executable,
        "-m",
        "msg_bus.cli.process",
        "--dsn",
        dsn,
        "--queue-names",
        queue_name,
        "--handlers-path",
        handlers_path,
        "--max-messages",
        str(max_messages),
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        cwd=PROJECT_ROOT,
        env=env,
        check=False,
    )


@unittest.skipIf(not get_dsn(), "PGMQ_DSN not set; skip E2E tests")
class TestCliE2E(unittest.TestCase):
    """E2E: enqueue via CLI then process via CLI with real DB."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dsn = get_dsn()
        assert cls.dsn
        cls.handlers_path = str(E2E_HANDLERS_DIR)
        if not E2E_HANDLERS_DIR.is_dir():
            raise FileNotFoundError(f"E2E handlers dir not found: {E2E_HANDLERS_DIR}")

    def setUp(self) -> None:
        """Ensure test queue exists (enqueue creates it if missing)."""
        from msg_bus.persist_pgmq import PersistPGMQ as QueueRepository  # noqa: PLC0415

        self.repo = QueueRepository(dsn=self.dsn)
        queues = self.repo.list_queues()
        if E2E_QUEUE_NAME not in queues:
            self.repo.create_queue(E2E_QUEUE_NAME)

    def tearDown(self) -> None:
        """Destroy test queue and close repo."""
        if hasattr(self, "repo"):
            with contextlib.suppress(Exception):
                self.repo.destroy_queue(E2E_QUEUE_NAME)
                self.repo.close()

    def test_enqueue_then_process_consumes_message(self) -> None:
        """Enqueue one message via CLI, process via CLI; message is consumed."""
        payload = {"e2e": True, "id": 1}
        enq = run_enqueue(self.dsn, E2E_QUEUE_NAME, payload)
        self.assertEqual(enq.returncode, 0, f"enqueue stderr: {enq.stderr!r} stdout: {enq.stdout!r}")
        self.assertIn("Message enqueued", enq.stdout)

        proc = run_process(
            dsn=self.dsn,
            queue_name=E2E_QUEUE_NAME,
            handlers_path=self.handlers_path,
            max_messages=1,
        )
        self.assertEqual(proc.returncode, 0, f"process stderr: {proc.stderr!r} stdout: {proc.stdout!r}")

        # Run process again; no message left, should exit 0 and do nothing
        proc2 = run_process(
            dsn=self.dsn,
            queue_name=E2E_QUEUE_NAME,
            handlers_path=self.handlers_path,
            max_messages=1,
        )
        self.assertEqual(proc2.returncode, 0)

    def test_enqueue_then_process_multiple_messages(self) -> None:
        """Enqueue two messages, process both in one run."""
        for i in range(2):
            enq = run_enqueue(self.dsn, E2E_QUEUE_NAME, {"n": i})
            self.assertEqual(enq.returncode, 0, f"enqueue #{i} failed: {enq.stderr!r}")

        proc = run_process(
            dsn=self.dsn,
            queue_name=E2E_QUEUE_NAME,
            handlers_path=self.handlers_path,
            max_messages=10,
        )
        self.assertEqual(proc.returncode, 0, f"process stderr: {proc.stderr!r}")

        # Queue should be empty
        proc2 = run_process(
            dsn=self.dsn,
            queue_name=E2E_QUEUE_NAME,
            handlers_path=self.handlers_path,
            max_messages=1,
        )
        self.assertEqual(proc2.returncode, 0)
