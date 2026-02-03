"""Process messages from one or more queues.

This module provides a CLI that dequeues messages, validates and/or handles them
via per-queue handlers, and archives or deletes them. Failed messages can be
re-enqueued with error metadata and a configurable visibility timeout.
"""

import importlib
import time
import traceback
import os
import sys
from typing import Any
from icecream import ic

import click

from config import get_settings
from msg_bus.persist_pgmq import PersistPGMQ as QueueRepository


def get_handlers(
    queue_names: list[str],
    queues: list[str],
    validate_only: bool = False,
    handlers_path: list[str] = [],
) -> dict[str, callable]:
    """Load and return the handler instance for each queue name.

    Args:
        queue_names: Queue names to load handlers for.
        queues: List of queue names that exist in the repository.
        validate_only: If True, require each handler to have a validate method.
        handlers_path: List of paths to search for handlers.
    Returns:
        Mapping of queue name to handler instance.

    Raises:
        click.ClickException: If a queue does not exist or (when validate_only)
            a handler has no validate method.
    """
    handlers: dict[str, callable] = {}
    for path in handlers_path:
        if os.path.exists(path):
            if path not in sys.path:
                sys.path.append(path)
    print(sys.path)
    for q in queue_names:
        if q not in queues:
            raise click.ClickException(f"Queue {q} does not exist")
        handler_module = importlib.import_module(f'handlers.{q}')
        handler = handler_module.Handler()
        handlers[q] = handler
        if not hasattr(handler, "validate") and validate_only:
            raise click.ClickException(f"No validator for queue: {q}")
    return handlers


def validate_message(message: dict, handlers: dict[str, callable], q: str) -> None:
    """Run the queue handler's validate method on the message, if present."""
    if hasattr(handlers[q], "validate"):
        handlers[q].validate(message)


def handle_message(message: dict, handlers: dict[str, callable], q: str) -> None:
    """Validate and then handle the message with the queue's handler."""
    if hasattr(handlers[q], "validate"):
        handlers[q].validate(message)
        handlers[q].handle(message)


@click.command()
@click.option("--dsn", type=str, required=False, help="The DSN of the database to use")
@click.option(
    "--max-messages",
    type=int,
    default=100,
    help="Maximum number of messages to process per queue",
)
@click.option("--max-runtime", type=int, default=600, help="Maximum runtime per queue in seconds")
@click.option(
    "--visibility-timeout",
    type=int,
    default=300,
    help="Visibility timeout in seconds for dequeued messages",
)
@click.option(
    "--error-visibility-timeout",
    type=int,
    default=601,
    help="Visibility timeout in seconds for re-queued error messages",
)
@click.option(
    "--queue-names",
    type=str,
    required=True,
    multiple=True,
    help="The name of a queue to process messages from, can be used multiple times",
)
@click.option(
    "--delete-messages",
    is_flag=True,
    default=False,
    help="Delete messages after processing, default is to archive them",
)
@click.option(
    "--validate-only",
    is_flag=True,
    help="Only validate the messages, do not process them",
)
@click.option(
    "--handlers-path",
    type=str,
    required=True,
    help="The path to a directory with a hanlers directory, multiple allowed",
    multiple=True,
)
def main(**kwargs: Any) -> None:
    """Process messages from the given queues.

    Dequeues messages from each named queue, validates and/or handles them
    with the corresponding handler, then archives or deletes them. With
    --validate-only, only validation is run and messages are not handled
    or removed.

    Visibility timeouts should be longer than the expected processing time
    per message; when the timeout expires, the message becomes visible again
    (e.g. if the processor died). Set error_visibility_timeout longer than
    max_runtime so failed messages re-enter in the next run cycle.
    """
    max_messages = kwargs["max_messages"]
    max_runtime = kwargs["max_runtime"]
    visibility_timeout = kwargs["visibility_timeout"]
    error_visibility_timeout = kwargs["error_visibility_timeout"]
    queue_names = list(kwargs["queue_names"])
    delete_messages = kwargs["delete_messages"]
    validate_only = kwargs["validate_only"]
    dsn = kwargs["dsn"]
    handlers_path = list(kwargs["handlers_path"])

    if not dsn:
        dsn = os.getenv("PGMQ_DSN", None)
    if not dsn:
        raise click.ClickException(f"No DSN provided and PGMQ_DSN environment variable is not set")
        
    try:
        queue_repo = QueueRepository(dsn=dsn)
        queues = queue_repo.list_queues()
        # get the handlers for the given queue names and
        handlers = get_handlers(
            list(queue_names), 
            queues, 
            validate_only=validate_only, 
            handlers_path=handlers_path,
        )
        # Process messages from each queue.
        for q in queue_names:
            # Cap runtime and message count so we don't overrun and miss future jobs.
            queue_start_time = time.time()
            message_count = 0
            while time.time() - queue_start_time < max_runtime and message_count < max_messages:
                message_count += 1
                message = queue_repo.dequeue(q, options={"visibility_timeout": visibility_timeout})
                if not message:
                    continue
                try:
                    validate_message(message, handlers, q)
                except Exception as e:
                    if validate_only:
                        click.secho(f"Validation error: {e}", err=True, color=True, fg="red")
                        click.secho(f"Stack trace: {traceback.format_exc()}", err=True, color=True, fg="red")
                        click.secho(f"Message: {message.message['data']}", err=True, color=True, fg="red")
                        continue
                try:
                    handle_message(message, handlers, q)
                    if delete_messages:
                        queue_repo.delete(q, message.msg_id)
                    else:
                        queue_repo.archive(q, message.msg_id)
                except Exception as e:
                    # Re-enqueue with error metadata and remove original so we can continue.
                    click.secho(f"Error handling message: {e}", err=True, color=True, fg="red")
                    message.message["meta"]["error_message"] = str(e)
                    message.message["meta"]["stack_trace"] = traceback.format_exc()
                    error_message_id = queue_repo.enqueue_error(
                        message.message,
                        message.msg_id,
                        queue_repo.queue,
                        visibility_timeout=error_visibility_timeout,
                    )
                    if error_message_id:
                        click.secho(f"Error message re-enqueued with ID: {error_message_id}", color=True, fg="green")
                    else:
                        click.secho(f"Error re-enqueuing message: {e}", err=True, color=True, fg="red")
                        raise e
    finally:
        queue_repo.close()


if __name__ == "__main__":
    main()
