# Message Bus Queues

The initial version uses the PostgreSQL pgmq extension and the related Python pgmq package to manage the message queue. It implements an interface so changing out the backend queue system should be fairly straightforward. Where options would apply to different backends, `options: dict[str, Any]` is used to allow flexibility. This code is basically a wrapper around python pgmq-py with structured metadata being added to the message. The message is split into data and meta: `{"data": {}, "meta": {}}`.

## Depends On

This project uses several packages but the true core of the queue system is pgmq.

- [Postgres pgmq extension](https://github.com/pgmq/pgmq)
- [Python pgmq-py package](https://github.com/pgmq/pgmq-py)

## DSN

You will need to export the DSN or use the `--dsn` parameter if using CLI tools.

```shell
PGMQ_DSN=postgresql://msg_bus:PASSWORD@localhost:5432/pgmq_d
```

A `.env` file in the working directory is loaded when `--dsn` is omitted. The port defaults to `5432` when it is not in the URL.

## Library API

```python
from msg_bus import (
    BaseHandler,
    DataDTO,
    MetaDTO,
    PersistBase,
    PersistPGMQ,
    QueueMessage,
    process_queues,
)
```

Enqueue with `PersistPGMQ.enqueue(DataDTO(...))`. Dequeue returns a `QueueMessage` (`msg_id` plus `payload: DataDTO`), not a pgmq-py object. `process_queues` runs the consume / validate / handle / archive loop for embedding in your own apps; the process CLI is a thin wrapper around it.

## Message Data

Data can be any serializable data the handler may need.

## Message Meta

Meta is intentionally light: `queue_name` for lookup, plus `error_message` and `stack_trace` for error tracking.

### Correlation ID and Correlation Queue

This is the ID and queue for the originating topic, think "employee hired" ID 5. If fan-out, the process will need a way to know when all tasks are completed. This is the ID that allows your process to verify that the tasks are completed. May or may not be needed in your implementation.

### Target ID

The ID of the object to be acted upon. At the task level the data needed should be provided by the process that adds the queue item. This is not intended for looking up additional data in the handler. It allows tracking of actions taken on a target across multiple queues.

### Version

The version of the message. Intended for handlers to know how to route message data while migrating formats.

The enqueue CLI can set these via `--correlation-id`, `--correlation-queue`, `--target-id`, and `--version`. Library callers set them on `MetaDTO`.

## Command Line Tools (CLI)

Trigger these with `uv run <tool> --help`.

- **msg-bus-enqueue** Adds an item to a queue (optional meta flags above).
- **msg-bus-queue** Manages queues with actions: `status` (prints metrics), `create`, `destroy`, `purge`. Example: `uv run msg-bus-queue --queue-name my_queue --action status`
- **msg-bus-process** Handles the messages in a queue. Stops a queue when it is empty (no busy-wait). Default is to archive after success; `--delete-messages` deletes instead. `--validate-only` validates without handling or removing messages.

### Handling Messages

Run the process CLI with `--handlers-path` pointing at a directory that contains a `handlers` package. For every queue you process you have an identically named module in that package (for example `handlers/exception_test.py` for the `exception_test` queue).

Handlers subclass `BaseHandler` and receive a dict `{"data": ..., "meta": ...}` (the payload, not the backend envelope). `handle` is required; `validate` is optional (default no-op). Raise from either to fail the message: handle failures are re-enqueued with error metadata and a longer visibility timeout.

Look at `tests/example_handlers/handlers/` for sample handlers.

## Testing

Tests mirror the source layout under `tests/`. CLI tests live in `tests/msg_bus/cli/`. Run tests with `uv run pytest`. Integration tests that talk to Postgres need `PGMQ_DSN` set; they skip when it is unset.
