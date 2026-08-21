"""Process messages from one or more queues.

CLI wrapper around :func:`msg_bus.processor.process_queues`.
"""

from typing import Any

import click

from msg_bus.dsn import resolve_dsn
from msg_bus.persist_pgmq import PersistPGMQ as QueueRepository
from msg_bus.processor import get_handlers, process_queues


def get_dsn(dsn: str | None) -> str:
    """Get DSN from argument or ``PGMQ_DSN``; raise ClickException if missing."""
    try:
        return resolve_dsn(dsn)
    except ValueError as err:
        raise click.ClickException(str(err)) from err


def _emit(level: str, text: str) -> None:
    if level == "error":
        click.secho(text, err=True, color=True, fg="red")
    else:
        click.secho(text, color=True, fg="green")


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
    help="The path to a directory with a handlers directory, multiple allowed",
    multiple=True,
)
def main(**kwargs: Any) -> None:
    """Process messages from the given queues.

    Dequeues messages from each named queue, validates and/or handles them
    with the corresponding handler, then archives or deletes them. With
    --validate-only, only validation is run and messages are not handled
    or removed. Processing a queue stops when it is empty.

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
    dsn = get_dsn(kwargs["dsn"])
    handlers_path = list(kwargs["handlers_path"])

    queue_repo = None
    try:
        queue_repo = QueueRepository(dsn=dsn)
        queues = queue_repo.list_queues()
        try:
            handlers = get_handlers(
                list(queue_names),
                queues,
                handlers_path=handlers_path,
                validate_only=validate_only,
            )
        except ValueError as err:
            raise click.ClickException(str(err)) from err
        process_queues(
            queue_repo,
            queue_names,
            handlers,
            max_messages=max_messages,
            max_runtime=max_runtime,
            visibility_timeout=visibility_timeout,
            error_visibility_timeout=error_visibility_timeout,
            delete_messages=delete_messages,
            validate_only=validate_only,
            emit=_emit,
        )
    finally:
        if queue_repo is not None:
            queue_repo.close()


if __name__ == "__main__":
    main()
