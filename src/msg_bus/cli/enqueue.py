"""Enqueue a message to a queue.

CLI that creates the queue if needed and sends a JSON message to it.
"""

import json

import click

from msg_bus.dsn import resolve_dsn
from msg_bus.exceptions import DuplicateTargetError
from msg_bus.persist_pgmq import PersistPGMQ as QueueRepository
from msg_bus.queue_model_dto import DataDTO, MetaDTO


def queue_exists(queue_repo: QueueRepository, queue_name: str) -> bool:
    """Return True if the given queue exists in the repository."""
    return queue_name in queue_repo.list_queues()


def get_dsn(dsn: str | None) -> str:
    """Get DSN from argument or ``PGMQ_DSN``; raise ClickException if missing."""
    try:
        return resolve_dsn(dsn)
    except ValueError as err:
        raise click.ClickException(str(err)) from err


@click.command()
@click.option(
    "--queue-name",
    type=str,
    required=True,
    help="Queue to send to, named for how work is processed (e.g. account_create, communication)",
)
@click.option("--message", type=str, required=True, help="The message to enqueue (JSON)")
@click.option("--dsn", type=str, required=False, help="The DSN of the database to use")
@click.option("--correlation-id", type=int, required=False, help="Correlation identifier for the originating topic")
@click.option("--correlation-queue", type=str, required=False, help="Queue name of the originating topic")
@click.option("--target-id", type=str, required=False, help="Identifier of the object being acted upon")
@click.option("--source-system", type=str, required=False, help="System that produced the message")
@click.option(
    "--action-type",
    type=str,
    required=False,
    help="Kind of change (add, update, remove, lock, or another value)",
)
@click.option(
    "--business-reason",
    type=str,
    required=False,
    help="Producer-defined reason the work was requested (free-form string)",
)
@click.option(
    "--associated-period",
    type=str,
    required=False,
    help="Optional academic period associated with the message (e.g. 2026FA)",
)
@click.option("--version", type=str, required=False, help="Message payload format version")
def main(  # noqa: PLR0913
    queue_name: str,
    message: str,
    dsn: str | None,
    correlation_id: int | None,
    correlation_queue: str | None,
    target_id: str | None,
    source_system: str | None,
    action_type: str | None,
    business_reason: str | None,
    associated_period: str | None,
    version: str | None,
) -> None:
    """Enqueue a JSON message to the specified queue; creates the queue if it does not exist."""
    click.echo(f"queue-name: {queue_name}")
    click.echo(f"message: {message}")
    dsn = get_dsn(dsn)

    queue_repo = None
    try:
        queue_repo = QueueRepository(dsn=dsn)
        if not queue_exists(queue_repo, queue_name):
            try:
                queue_repo.create_queue(queue_name)
            except Exception as e:
                raise click.ClickException(f"Error creating queue: {e}") from e

        try:
            data = json.loads(message)
            meta = MetaDTO(
                queue_name=queue_name,
                correlation_id=correlation_id,
                correlation_queue=correlation_queue,
                target_id=target_id,
                source_system=source_system,
                action_type=action_type,
                business_reason=business_reason,
                associated_period=associated_period,
                version=version,
            )
            message_data = DataDTO(data=data, meta=meta)
            message_id = queue_repo.enqueue(message_data)
            click.echo(f"Message enqueued with ID: {message_id}")
        except json.JSONDecodeError as err:
            raise click.ClickException(f"Invalid JSON: {message}") from err
        except DuplicateTargetError as err:
            raise click.ClickException(str(err)) from err
        except click.ClickException:
            raise
        except Exception as e:
            raise click.ClickException(f"Error: {e}") from e
    finally:
        if queue_repo is not None:
            queue_repo.close()


if __name__ == "__main__":
    """Entry point for the enqueue CLI."""
    main()
