"""Show the status of a queue.

CLI that prints metrics (e.g. message counts) for a given queue name.
"""

import click

from config import get_settings
from mb_queue.persist_pgmq import PersistPGMQ as QueueRepository


def queue_exists(queue_repo: QueueRepository, queue_name: str) -> bool:
    """Return True if the given queue exists in the repository."""
    return queue_name in queue_repo.list_queues()


@click.command()
@click.option("--queue-name", type=str, required=True, help="The name of the queue to show the status of")
def main(queue_name: str) -> None:
    """Print metrics for the specified queue (e.g. total, visible, archived)."""
    click.echo("Queue status")

    settings = get_settings()
    queue_repo = QueueRepository(dsn=settings.pgmq_dsn)
    try:
        if not queue_exists(queue_repo, queue_name):
            raise click.ClickException(f"Queue {queue_name} does not exist")

        metrics = queue_repo.metrics(queue_name)
        print(metrics)
    finally:
        queue_repo.close()


if __name__ == "__main__":
    main()
