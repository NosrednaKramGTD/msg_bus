"""Show the status of a queue.

CLI that prints metrics (e.g. message counts) for a given queue name.
"""

import os

import click
import dotenv
from icecream import ic

from msg_bus.persist_pgmq import PersistPGMQ as QueueRepository


def queue_exists(queue_repo: QueueRepository, queue_name: str) -> bool:
    """Return True if the given queue exists in the repository."""
    return queue_name in queue_repo.list_queues()


@click.command()
@click.option("--queue-name", type=str, required=True, help="The name of the queue to show the status of")
@click.option("--dsn", type=str, required=False, help="The DSN of the database to use")
def main(queue_name: str, dsn: str) -> None:
    """Print metrics for the specified queue (e.g. total, visible, archived)."""
    click.echo("Queue status")

    if not dsn:
        if os.path.exists(".env"):
            dotenv.load_dotenv()
        dsn = os.getenv("PGMQ_DSN")
    if not dsn:
        raise click.ClickException("No DSN provided no .env file found")

    queue_repo = QueueRepository(dsn=dsn)
    try:
        if not queue_exists(queue_repo, queue_name):
            raise click.ClickException(f"Queue {queue_name} does not exist")

        metrics = queue_repo.metrics(queue_name)
        ic(metrics)
    finally:
        queue_repo.close()


if __name__ == "__main__":
    main()
