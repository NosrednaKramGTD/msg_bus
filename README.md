# Message Bus Queues

The initial version uses the PostgreSQL pgmq extension and the related Python pgmq package to manage the message queue. It implements an interface so changing out the backend queue system should be fairly straightforward. Where options would apply to different backends, `options: dict[str, Any]` is used to allow flexibility. This code is basically a wrapper around python pgmq-py with structured metadata being added to the message. The message is split into data and meta: `{"data": {}, "meta": {}}`.

## Depends On

This project uses several packages but the true core of the queue system is pgmq.

- [Postgres pgmq extension](https://github.com/pgmq/pgmq)
- [Python pgmq-py package](https://github.com/pgmq/pgmq-py)

## DevOps: low-level queue tasks

The installed CLIs are the ops surface: create and inspect queues, inject a one-off message, drain or validate a queue, purge, or drop it. From this repo use `uv run <tool>`; after install the same commands are on `PATH`. Every tool accepts `--dsn`; if omitted it uses `PGMQ_DSN` (and loads `.env` from the working directory). Port defaults to `5432` when it is not in the URL.

```shell
export PGMQ_DSN=postgresql://msg_bus:PASSWORD@localhost:5432/pgmq_d
uv run msg-bus-queue --help
uv run msg-bus-enqueue --help
uv run msg-bus-process --help
```

**Create / inspect / empty / drop a queue** (`msg-bus-queue`). `status` prints metrics (length, ages, totals). `purge` removes visible messages but keeps the queue. `destroy` drops the queue and its data.

```shell
uv run msg-bus-queue --queue-name my_queue --action create
uv run msg-bus-queue --queue-name my_queue --action status
uv run msg-bus-queue --queue-name my_queue --action purge
uv run msg-bus-queue --queue-name my_queue --action destroy
```

**Inject a message** (`msg-bus-enqueue`). `--message` is a JSON object stored as `data`. The queue is created if it does not exist. Name the queue for **how** it is processed (`account_create`, `communication`, `account_update`). Optional meta: `--correlation-id`, `--correlation-queue`, `--target-id`, `--source-system`, `--action-type`, `--business-reason`, `--associated-period`, `--event-key`, `--version`.

```shell
uv run msg-bus-enqueue --queue-name my_queue --message '{"id": 1, "action": "retry"}'
```

**Drain or validate** (`msg-bus-process`). Requires `--handlers-path` pointing at a directory that contains a `handlers` package, with one module per queue name. Processing a queue **stops when it is empty** (it does not sit for `--max-runtime` if there is nothing to do). Successful messages are **archived** by default; pass `--delete-messages` to delete instead. Failed messages are re-enqueued with error metadata and `--error-visibility-timeout` (default 601s). Set that longer than `--max-runtime` so failures show up on the next run.

```shell
# Process up to 100 messages (default), then exit
uv run msg-bus-process --queue-names my_queue --handlers-path /path/to/app

# Several queues in one run
uv run msg-bus-process --queue-names account_create --queue-names communication --handlers-path /path/to/app

# Validate payloads/handlers without handling or removing messages
uv run msg-bus-process --queue-names my_queue --handlers-path /path/to/app --validate-only
```

`--visibility-timeout` (default 300s) must exceed expected handle time. If the process dies mid-message, the item becomes visible again after that timeout.

Pass `--dsn` instead of the env var when you need a one-off connection:

```shell
uv run msg-bus-queue --dsn "$PGMQ_DSN" --queue-name my_queue --action status
```

## Developers: enqueue and handle messages

Applications **enqueue** with `PersistPGMQ` and `DataDTO`. They **handle** via a `handlers.<queue_name>` module with a `Handler` class. Ops usually drain those queues with `msg-bus-process` (see [DevOps](#devops-low-level-queue-tasks)); you can also call `process_queues` from your own process.

```mermaid
flowchart LR
  producer[Producer app] --> enqueue["PersistPGMQ.enqueue"]
  enqueue --> pgmq[(PostgreSQL PGMQ)]
  processCLI[msg-bus-process] --> dequeue["dequeue"]
  dequeue --> pgmq
  dequeue --> handler["handlers.queue_name.Handler"]
  handler -->|success| archive[archive or delete]
  handler -->|raise| deadLetter[enqueue_error plus VT]
```

**Key points**

- Stored shape is always `{"data": {...}, "meta": {...}}`. Handlers receive that **dict**, not a pgmq or `QueueMessage` object.
- Put everything the handler needs in `data`. Do not look up extra records in the handler; the producer is responsible for the payload.
- `meta.queue_name` must be the queue you send to, named for **how** work is processed (`account_create`, `communication`, `account_update`). Use `business_reason` for **why** (a producer-defined string; the bus does not enumerate events), optional `associated_period` for the academic term, `correlation_id` / `correlation_queue` for fan-out, `target_id` for the object acted on, `source_system` for the producing system, `action_type` for the kind of change (`add` / `update` / `remove` / `lock`), `event_key` for an occurrence used in archive lookup, `version` when the payload format changes. Payload fields such as `preferred_delivery_method` (`SMS`, `EMAIL`) live in `data` by queue convention, not in meta.
- `enqueue` rejects a second pending or in-flight message with the same `queue_name` + `target_id` (`DuplicateTargetError`). Omit `target_id` to skip de-dupe. After archive or delete, a later event for that target can enqueue.
- `find_archived_event(queue_name, event_key)` returns the newest archived `msg_id` for that producer-defined key, or `None`. Use it to skip a second `account_create` for the same hire occurrence without blocking a later re-hire (new `event_key`, same `target_id`). `enqueue` does not reject archived keys. Omit `event_key` when you do not need this lookup. Keep default archive (not `--delete-messages`) on create-once queues or completed work will not be found. Handlers should still no-op if the account already exists.
- Handler file name equals the queue name. The class must be named `Handler` and subclass `BaseHandler`. `handle` is required; `validate` is optional (default no-op). **Raise** from either to fail the message.
- On failure the processor re-enqueues with `error_message` and `stack_trace` and a longer visibility timeout. Invalid stored JSON never reaches `handle`; it is dead-lettered the same way.
- Always `close()` the repository when your producer is done.

### Enqueue from application code

```python
from msg_bus import ActionType, DataDTO, MetaDTO, PersistPGMQ

repo = PersistPGMQ()  # or PersistPGMQ(dsn="postgresql://...")
try:
    repo.create_queue("account_create")  # skip if ops already created the queue
    repo.create_queue("communication")
    event_key = "workday:hire:E123:2026-08-25"
    archived_id = repo.find_archived_event("account_create", event_key)
    if archived_id is not None:
        pass  # this hire occurrence already completed on this bus
    else:
        msg_id = repo.enqueue(
            DataDTO(
                data={"employee_id": "E123", "email": "a@example.edu"},
                meta=MetaDTO(
                    queue_name="account_create",
                    correlation_id=5,
                    correlation_queue="employee_lifecycle",
                    target_id="E123",
                    source_system="workday",
                    action_type=ActionType.ADD,
                    business_reason="hire",
                    associated_period="2026FA",
                    event_key=event_key,
                    version="1",
                ),
            )
        )
    repo.enqueue(
        DataDTO(
            data={
                "employee_id": "E123",
                "preferred_delivery_method": "EMAIL",
                "template": "welcome",
            },
            meta=MetaDTO(
                queue_name="communication",
                correlation_id=5,
                correlation_queue="employee_lifecycle",
                target_id="E123",
                source_system="workday",
                action_type=ActionType.ADD,
                business_reason="hire",
            ),
        )
    )
finally:
    repo.close()
```

```mermaid
flowchart TD
  build["Build DataDTO data plus MetaDTO"] --> send["repo.enqueue"]
  send --> stored["PGMQ row: data and meta JSON"]
```

### Write a handler

`--handlers-path` (or `get_handlers`) is the **parent** of a `handlers` package. Queue `account_create` loads `handlers.account_create.Handler`:

```text
my_app/
  handlers/
    __init__.py
    account_create.py
```

```python
# handlers/account_create.py
from msg_bus import BaseHandler


class Handler(BaseHandler):
    def validate(self, message: dict) -> None:
        if "employee_id" not in message["data"]:
            raise ValueError("employee_id is required")

    def handle(self, message: dict) -> None:
        employee_id = message["data"]["employee_id"]
        # use only fields in message["data"] / message["meta"]
        provision_account(employee_id)
```

```mermaid
flowchart TD
  read["dequeue QueueMessage"] --> shape{"payload is DataDTO?"}
  shape -->|no| err[enqueue_error]
  shape -->|yes| val["Handler.validate dict"]
  val --> work["Handler.handle dict"]
  work -->|ok| done[archive or delete]
  work -->|raise| err
  val -->|raise| err
```

Run it with the process CLI, or embed the same loop:

```python
from msg_bus import PersistPGMQ, process_queues
from msg_bus.processor import get_handlers

repo = PersistPGMQ()
try:
    names = ["account_create"]
    handlers = get_handlers(names, repo.list_queues(), handlers_path=["/path/to/my_app"])
    process_queues(repo, names, handlers, max_messages=100, max_runtime=600)
finally:
    repo.close()
```

See `tests/example_handlers/handlers/` for minimal samples. Field meanings for `meta` are under [Message Meta](#message-meta).

## Library API

Producer and handler examples are in [Developers: enqueue and handle messages](#developers-enqueue-and-handle-messages). Public imports:

```python
from msg_bus import (
    ActionType,
    BaseHandler,
    DataDTO,
    DuplicateTargetError,
    MetaDTO,
    PersistBase,
    PersistPGMQ,
    QueueMessage,
    process_queues,
)
```

`dequeue` returns a `QueueMessage` (`msg_id` plus `payload: DataDTO` when valid). `process_queues` is the consume / validate / handle / archive loop; the process CLI wraps it. `find_archived_event(queue_name, event_key)` returns the newest archived `msg_id` for that key, or `None`.

## Message Data

Data can be any serializable data the handler may need. The bus does not schema-validate `data`; producers and handlers agree on field names **by convention** for each queue. Handler fields stay flat in `data`.

Examples of queue-specific conventions:

- **communication:** `preferred_delivery_method` (`SMS`, `EMAIL`, …), plus template or body fields the handler needs.
- **account_create / account_update:** identity and account fields the handler needs (for example `employee_id`, `email`).

For later research, producers *may* add snapshots without changing the handler contract:

- **update / remove:** `data["old"]` is the before image. Optionally `data["new"]` when you want an explicit pair; otherwise the rest of `data` is the current state.
- **add:** current state only.
- **lock:** `meta.action_type` plus whatever the handler needs; no required snapshots.

The bus does not validate that `update` has `old` / `new`. Put snapshots in `--message` JSON (there are no `--old` / `--new` flags). PGMQ archive is suitable for light JSONB research (`message->'meta'->>'action_type'`), not a long-term analytics warehouse.

## Message Meta

Meta is intentionally light: `queue_name` for lookup, plus `error_message` and `stack_trace` for error tracking. Use meta for **which stream** and **why**; put handler payload (including delivery preferences) in `data`.

### Queue Name

Name the queue for **how** the message is processed (the handler / work stream), not for the business event that triggered it. Examples: `account_create`, `account_update`, `communication`. Do not name queues `hire` or `terminated`; those belong on `business_reason`. Handler file name equals this queue name (`handlers.account_create.Handler`).

### Correlation ID and Correlation Queue

This is the ID and queue for the originating topic, think employee lifecycle ID 5. If fan-out, the process will need a way to know when all tasks are completed. This is the ID that allows your process to verify that the tasks are completed. May or may not be needed in your implementation. It is not used for archive lookup; a re-hire often keeps the same lifecycle id. Use `event_key` for occurrence identity.

### Target ID

The ID of the object to be acted upon. At the task level the data needed should be provided by the process that adds the queue item. This is not intended for looking up additional data in the handler. It allows tracking of actions taken on a target across multiple queues.

When `target_id` is set, `enqueue` is first-wins on that queue: a pending or in-flight message with the same `queue_name` and `target_id` is rejected (`DuplicateTargetError`) so duplicates cannot run out of sequence or apply twice downstream. Archived and deleted messages do not count. Leave `target_id` unset to skip de-dupe. Failure re-queue (`enqueue_error`) does not use this check.

Do not use `target_id` to decide whether a create-once job already completed. It is the person; a re-hire is the same `target_id`.

### Event Key

Optional producer-defined string that identifies **this occurrence** of work, not the person (`target_id`) and not the originating topic (`correlation_id`). The bus does not parse or validate it. Set it when you want to ask “did this event already complete on this queue?” after success has been archived.

Convention example: `{source_system}:{business_reason}:{target_id}:{event_date}` such as `workday:hire:E123:2026-08-25`. Same hire retried → same key → `find_archived_event` returns a `msg_id` and the producer can skip. Later re-hire → new date in the key → miss → new `account_create` is allowed. Omit the field when you do not need archive lookup.

`find_archived_event(queue_name, event_key)` searches `pgmq.a_{queue_name}` only (pending rows are ignored) and returns the newest matching `msg_id`, or `None`. Blank keys are not queried. Lookup is per work queue, so the same key on `account_create` and `communication` is independent. `enqueue` does not reject archived keys.

This is a hint, not “the account exists.” Check-then-enqueue can race; work done outside the bus or with `--delete-messages` will not be in archive. Handlers should still no-op if the account already exists. Archive scans grow with table size; if lookup is hot, add an expression index on `(message->'meta'->>'event_key')`.

### Source System

The system that produced the message (for example Workday, Banner, or a local job). Optional tracking for ops and handlers; it is not part of duplicate detection.

### Action Type

The kind of change on this queue: canonical values are `add`, `update`, `remove`, and `lock` (`ActionType` enum). Other strings (`unlock`, `merge`, …) are allowed so producers do not wait on a library bump. Optional; not used for duplicate detection. Queue name remains the work stream (`account_create`, `communication`); `action_type` is for reporting.

### Business Reason

Why the work was requested. A producer-defined string; the bus does not enumerate or validate values so it stays independent of institutional events. Optional; not used for duplicate detection. Example: a hire that provisions an account uses queue `account_create` with `business_reason="hire"`. The same hire can also enqueue `communication` with the same reason.

### Associated Period

Optional academic period tied to the message (for example `2026FA` or `202610`). Student data is usually associated with a term; staff events may omit this. Not used for duplicate detection.

### Version

The version of the message. Intended for handlers to know how to route message data while migrating formats.

The enqueue CLI can set these via `--correlation-id`, `--correlation-queue`, `--target-id`, `--source-system`, `--action-type`, `--business-reason`, `--associated-period`, `--event-key`, and `--version`. Library callers set them on `MetaDTO`.

## Command Line Tools (CLI)

Ops recipes (DSN, create/status/purge/destroy, enqueue, drain) are in [DevOps: low-level queue tasks](#devops-low-level-queue-tasks). `uv run <tool> --help` lists flags.

### Handling Messages

Handler layout, `BaseHandler` contract, and `process_queues` usage are in [Developers: enqueue and handle messages](#developers-enqueue-and-handle-messages).

## Testing

Tests mirror the source layout under `tests/`. CLI tests live in `tests/msg_bus/cli/`. Run tests with `uv run pytest`. Integration tests that talk to Postgres need `PGMQ_DSN` set; they skip when it is unset.
