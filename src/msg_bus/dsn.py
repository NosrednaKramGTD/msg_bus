"""DSN resolution and parsing for PGMQ connections."""

import os
from typing import Any
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv
from pydantic import PostgresDsn, TypeAdapter, ValidationError

_DSN_ADAPTER = TypeAdapter(PostgresDsn)


def resolve_dsn(dsn: str | None = None) -> str:
    """Return an explicit DSN, else ``PGMQ_DSN`` from the environment.

    Loads ``.env`` from the current working directory when no DSN argument
    is given. Raises ValueError if nothing is available.
    """
    if dsn:
        return dsn
    if os.path.exists(".env"):
        load_dotenv()
    env_dsn = os.getenv("PGMQ_DSN")
    if not env_dsn:
        raise ValueError("No DSN provided and PGMQ_DSN is not set")
    return env_dsn


def parse_pgmq_dsn(dsn: str) -> dict[str, Any]:
    """Validate a Postgres DSN and return PGMQueue connection fields.

    Defaults the port to 5432 when omitted and URL-decodes user/password.
    """
    if not dsn or not str(dsn).strip() or str(dsn) == "None":
        raise ValueError("A Postgres DSN is required (pass dsn or set PGMQ_DSN)")
    try:
        _DSN_ADAPTER.validate_python(dsn)
    except ValidationError as err:
        raise ValueError(f"Invalid Postgres DSN: {err}") from err

    parts = urlparse(dsn)
    if not parts.hostname:
        raise ValueError("Postgres DSN must include a host")
    database = (parts.path or "").lstrip("/")
    if not database:
        raise ValueError("Postgres DSN must include a database name")
    return {
        "host": parts.hostname,
        "port": parts.port or 5432,
        "database": database,
        "username": unquote(parts.username) if parts.username else "",
        "password": unquote(parts.password) if parts.password else "",
    }
