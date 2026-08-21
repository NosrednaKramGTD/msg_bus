"""Process messages from one or more queues.

Dequeues messages, validates and/or handles them via per-queue handlers,
and archives or deletes them. Failed messages are re-enqueued with error
metadata and a configurable visibility timeout.
"""

from __future__ import annotations

import importlib
import logging
import os
import sys
import time
import traceback
from collections.abc import Callable
from typing import Any

from msg_bus.persist_base import PersistBase

logger = logging.getLogger(__name__)

EmitFn = Callable[[str, str], None]


def get_handlers(
    queue_names: list[str],
    queues: list[str],
    handlers_path: list[str],
    validate_only: bool = False,
) -> dict[str, Any]:
    """Load and return the handler instance for each queue name.

    Args:
        queue_names: Queue names to load handlers for.
        queues: List of queue names that exist in the repository.
        handlers_path: Directories that contain a ``handlers`` package.
        validate_only: If True, require each handler to have a validate method.

    Returns:
        Mapping of queue name to handler instance.

    Raises:
        ValueError: If a queue does not exist or (when validate_only)
            a handler has no validate method.
    """
    handlers: dict[str, Any] = {}
    for path in handlers_path:
        if os.path.exists(path) and path not in sys.path:
            sys.path.append(path)
    for q in queue_names:
        if q not in queues:
            raise ValueError(f"Queue {q} does not exist")
        handler_module = importlib.import_module(f"handlers.{q}")
        handler = handler_module.Handler()
        handlers[q] = handler
        if validate_only and not callable(getattr(handler, "validate", None)):
            raise ValueError(f"No validator for queue: {q}")
    return handlers


def validate_message(message: dict, handlers: dict[str, Any], q: str) -> None:
    """Run the queue handler's validate method on the message, if present."""
    validate = getattr(handlers[q], "validate", None)
    if callable(validate):
        validate(message)


def handle_message(message: dict, handlers: dict[str, Any], q: str) -> None:
    """Validate (if present) and then handle the message with the queue's handler."""
    validate_message(message, handlers, q)
    handlers[q].handle(message)


def _coerce_payload_dict(raw: Any) -> dict[str, Any]:
    """Normalize stored JSON into a ``{"data", "meta"}`` dict for dead-lettering."""
    if hasattr(raw, "model_dump"):
        return raw.model_dump()
    if isinstance(raw, dict):
        if "data" in raw or "meta" in raw:
            data = raw.get("data")
            meta = raw.get("meta")
            return {
                "data": data if isinstance(data, dict) else {"_invalid": data},
                "meta": dict(meta) if isinstance(meta, dict) else {},
            }
        return {"data": raw, "meta": {}}
    return {"data": {"_invalid": raw}, "meta": {}}


def _payload_dict(message: Any) -> dict[str, Any]:
    """Return the handler payload as a ``{"data", "meta"}`` dict.

    Raises ValueError when the dequeued message did not match DataDTO so the
    process loop can dead-letter it instead of calling the handler.
    """
    payload = getattr(message, "payload", None)
    if payload is None:
        raise ValueError(f"Invalid message payload: {getattr(message, 'raw_payload', None)!r}")
    if hasattr(payload, "model_dump"):
        return payload.model_dump()
    if isinstance(payload, dict):
        return payload
    raise TypeError("Dequeued message payload must be DataDTO or dict")


def _emit(emit: EmitFn | None, level: str, text: str) -> None:
    if emit:
        emit(level, text)
    elif level == "error":
        logger.error(text)
    else:
        logger.info(text)


def process_queues(  # noqa: PLR0913
    queue_repo: PersistBase,
    queue_names: list[str],
    handlers: dict[str, Any],
    *,
    max_messages: int = 100,
    max_runtime: int = 600,
    visibility_timeout: int = 300,
    error_visibility_timeout: int = 601,
    delete_messages: bool = False,
    validate_only: bool = False,
    emit: EmitFn | None = None,
) -> None:
    """Process messages from each named queue until empty, capped, or timed out.

    Stops a queue as soon as dequeue returns None (no busy-wait). Handlers
    receive the payload dict ``{"data": ..., "meta": ...}``. With
    ``validate_only``, messages are validated but not handled or removed.
    """
    dequeue_opts = {"visibility_timeout": visibility_timeout}
    if validate_only:
        _validate_queues(queue_repo, queue_names, handlers, dequeue_opts, emit)
        return

    for q in queue_names:
        queue_start_time = time.time()
        message_count = 0
        while time.time() - queue_start_time < max_runtime and message_count < max_messages:
            message = queue_repo.dequeue(q, options=dequeue_opts)
            if not message:
                break
            message_count += 1
            try:
                payload = _payload_dict(message)
                handle_message(payload, handlers, q)
                if delete_messages:
                    queue_repo.delete(q, message.msg_id)
                else:
                    queue_repo.archive(q, message.msg_id)
            except Exception as exc:
                _emit(emit, "error", f"Error handling message: {exc}")
                payload = _error_payload(message, exc, queue_name=q)
                error_message_id = queue_repo.enqueue_error(
                    q,
                    message.msg_id,
                    payload,
                    visibility_timeout=error_visibility_timeout,
                )
                if error_message_id is not None:
                    _emit(emit, "info", f"Error message re-enqueued with ID: {error_message_id}")
                else:
                    _emit(emit, "error", f"Error re-enqueuing message: {exc}")
                    raise


def _error_payload(message: Any, exc: Exception, queue_name: str = "") -> dict[str, Any]:
    """Attach error metadata onto a copy of the message payload."""
    payload_obj = getattr(message, "payload", None)
    raw = payload_obj if payload_obj is not None else getattr(message, "raw_payload", None)
    payload = _coerce_payload_dict(raw)
    meta = payload.setdefault("meta", {})
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta
    if queue_name:
        meta.setdefault("queue_name", queue_name)
    meta["error_message"] = str(exc)
    meta["stack_trace"] = traceback.format_exc()
    return payload


def _validate_queues(
    queue_repo: PersistBase,
    queue_names: list[str],
    handlers: dict[str, Any],
    dequeue_opts: dict[str, Any],
    emit: EmitFn | None,
) -> None:
    """Validate dequeued messages without handling or removing them."""
    for q in queue_names:
        while True:
            message = queue_repo.dequeue(q, options=dequeue_opts)
            if not message:
                break
            try:
                payload = _payload_dict(message)
                validate_message(payload, handlers, q)
            except Exception as exc:
                _emit(emit, "error", f"Validation error: {exc}")
                _emit(emit, "error", f"Stack trace: {traceback.format_exc()}")
                try:
                    data = _payload_dict(message).get("data")
                except Exception:
                    data = None
                _emit(emit, "error", f"Message: {data}")
