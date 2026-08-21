"""Tests for DSN resolution and parsing."""

from unittest import TestCase
from unittest.mock import patch

from msg_bus.dsn import parse_pgmq_dsn, resolve_dsn


class TestParsePgmqDsn(TestCase):
    def test_defaults_port_and_unquotes_password(self):
        parsed = parse_pgmq_dsn("postgresql://user:p%40ss@localhost/mydb")
        self.assertEqual(parsed["host"], "localhost")
        self.assertEqual(parsed["port"], 5432)
        self.assertEqual(parsed["database"], "mydb")
        self.assertEqual(parsed["username"], "user")
        self.assertEqual(parsed["password"], "p@ss")

    def test_keeps_explicit_port(self):
        parsed = parse_pgmq_dsn("postgresql://user:pass@db.example:6543/app")
        self.assertEqual(parsed["port"], 6543)
        self.assertEqual(parsed["host"], "db.example")

    def test_rejects_missing_dsn(self):
        with self.assertRaises(ValueError) as ctx:
            parse_pgmq_dsn("")
        self.assertIn("required", str(ctx.exception).lower())

        with self.assertRaises(ValueError):
            parse_pgmq_dsn("None")

    def test_rejects_invalid_dsn(self):
        with self.assertRaises(ValueError):
            parse_pgmq_dsn("not-a-dsn")


class TestResolveDsn(TestCase):
    def test_prefers_argument(self):
        self.assertEqual(resolve_dsn("postgresql://explicit/db"), "postgresql://explicit/db")

    @patch("msg_bus.dsn.os.getenv", return_value=None)
    @patch("msg_bus.dsn.os.path.exists", return_value=False)
    def test_raises_when_missing(self, _exists, _getenv):
        with self.assertRaises(ValueError) as ctx:
            resolve_dsn(None)
        self.assertIn("No DSN provided", str(ctx.exception))

    @patch("msg_bus.dsn.os.getenv", return_value="postgresql://from-env/db")
    @patch("msg_bus.dsn.os.path.exists", return_value=False)
    def test_uses_env_when_arg_missing(self, _exists, _getenv):
        self.assertEqual(resolve_dsn(None), "postgresql://from-env/db")
