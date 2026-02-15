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
@click.option("--action", type=str, required=True, help="The action to perform on the queue")
def main(queue_name: str, dsn: str, action: str = "status") -> bool | dict | int:
    """Print metrics for the specified queue (e.g. total, visible, archived)."""
    click.echo(f"Queue {queue_name} {action}")

    if not dsn:
        if os.path.exists(".env"):
            dotenv.load_dotenv()
        dsn = os.getenv("PGMQ_DSN")
    if not dsn:
        raise click.ClickException("No DSN provided no .env file found")

    try:
        queue_repo = QueueRepository(dsn=dsn)
        match action:
            case "create":
                queue_repo.create_queue(queue_name)
                click.echo(f"Queue {queue_name} created")
                return
            case "status":
                metrics = queue_repo.metrics(queue_name)
                ic(metrics)
                return metrics
            case "destroy":
                queue_repo.destroy_queue(queue_name)
                click.echo(f"Queue {queue_name} destroyed")
                return True
            case "purge":
                purged_count = queue_repo.purge_queue(queue_name)
                click.echo(f"Queue {queue_name} purged")
                return purged_count
            case _:
                raise click.ClickException(
                    f"Invalid action: {action}. Valid actions are: create, status, destroy, purge"
                )
    finally:
        queue_repo.close()


if __name__ == "__main__":
    main()
