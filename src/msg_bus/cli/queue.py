"""Show the status of a queue.

CLI that prints metrics (e.g. message counts) for a given queue name.
"""

import click

from msg_bus.dsn import resolve_dsn
from msg_bus.persist_pgmq import PersistPGMQ as QueueRepository


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
@click.option("--queue-name", type=str, required=True, help="The name of the queue to show the status of")
@click.option("--dsn", type=str, required=False, help="The DSN of the database to use")
@click.option(
    "--action",
    type=click.Choice(["create", "status", "destroy", "purge"], case_sensitive=False),
    required=True,
    help="The action to perform on the queue",
)
def main(queue_name: str, dsn: str | None, action: str) -> None:
    """Create, inspect, purge, or destroy a queue."""
    action = action.lower()
    click.echo(f"Queue {queue_name} {action}")
    dsn = get_dsn(dsn)

    queue_repo = None
    try:
        queue_repo = QueueRepository(dsn=dsn)
        match action:
            case "create":
                queue_repo.create_queue(queue_name)
                click.echo(f"Queue {queue_name} created")
            case "status":
                metrics = queue_repo.metrics(queue_name)
                click.echo(metrics)
            case "destroy":
                queue_repo.destroy_queue(queue_name)
                click.echo(f"Queue {queue_name} destroyed")
            case "purge":
                queue_repo.purge_queue(queue_name)
                click.echo(f"Queue {queue_name} purged")
    finally:
        if queue_repo is not None:
            queue_repo.close()


if __name__ == "__main__":
    main()
