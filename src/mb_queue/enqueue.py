"""Enqueue a message to a queue.

CLI that creates the queue if needed and sends a JSON message to it.
"""

import json

import click

from config import get_settings
from mb_queue.persist_pgmq import PersistPGMQ as QueueRepository
from mb_queue.queue_model_dto import DataDTO


def queue_exists(queue_repo: QueueRepository, queue_name: str) -> bool:
    """Return True if the given queue exists in the repository."""
    return queue_name in queue_repo.list_queues()


@click.command()
@click.option(
    "--queue-name",
    type=str,
    required=True,
    help="The name of the queue to enqueue the message to",
)
@click.option("--message", type=str, required=True, help="The message to enqueue (JSON)")
def main(queue_name: str, message: str) -> None:
    """Enqueue a JSON message to the specified queue; creates the queue if it does not exist."""
    click.echo(f"queue-name: {queue_name}")
    click.echo(f"message: {message}")

    settings = get_settings()
    queue_repo = QueueRepository(dsn=settings.pg_dsn)
    try:
        if not queue_exists(queue_repo, queue_name):
            try:
                queue_repo.create_queue(queue_name)
            except Exception as e:
                raise click.ClickException(f"Error creating queue: {e}") from e

        try:
            data = json.loads(message)
            meta = {
                "queue_name": queue_name,
            }
            message_data = DataDTO(data=data, meta=meta)
            message_id = queue_repo.enqueue(message_data)
            click.echo(f"Message enqueued with ID: {message_id}")
        except json.JSONDecodeError as err:
            raise click.ClickException(f"Invalid JSON: {message}") from err
        except Exception as e:
            raise click.ClickException(f"Error: {e}") from e
    finally:
        queue_repo.close()


if __name__ == "__main__":
    """Entry point for the enqueue CLI."""
    main()
