
**Queue naming and meta**

* Queue names describe **how** work is processed (`account_create`, `communication`, `account_update`), not the business event. `MetaDTO.business_reason` is a free-form string so the bus is not coupled to institutional events. Optional `MetaDTO.associated_period` records the academic term. Delivery fields such as `preferred_delivery_method` (`SMS`, `EMAIL`) stay in `data` by queue convention. Enqueue CLI: `--business-reason`, `--associated-period`.

**Enqueue de-dupe (behavior change)**

* `enqueue` rejects a new message when the same queue already has a pending or in-flight row with the same `meta.target_id`. Raises `DuplicateTargetError`. Omit `target_id` to skip the check. `enqueue_error` is unchanged. Archive or delete clears the slot.

**Meta**

* `MetaDTO.source_system` records the producing system. Optional on enqueue (`--source-system` on the CLI). Not used for duplicate detection.
* `MetaDTO.action_type` records the kind of change (`add` / `update` / `remove` / `lock`, or another string). Optional (`--action-type`). Old/new snapshots stay in `data` (`data["old"]` / optional `data["new"]`); they are not meta fields. Not used for duplicate detection.

**Library API (breaking if you dequeue / error-requeue in your own code)**

* `dequeue` now returns `QueueMessage` (`msg_id` + `payload: DataDTO`), not `<span class="ui-markdown__inline-path-filename ui-1heor9g md-inline-path-filename">pgmq.Message</span>`. `<span class="ui-markdown__inline-path-filename ui-1heor9g md-inline-path-filename">message.message</span>` no longer exists; use `<span class="ui-markdown__inline-path-filename ui-1heor9g md-inline-path-filename">message.payload</span>` (or `message.payload.model_dump()` for the dict).
* `enqueue_error` is now `(queue_name, message_id, payload, visibility_timeout=...)`. Passing a pgmq queue object is gone.
* `metrics` now returns a `dict`. Attribute access like `<span class="ui-markdown__inline-path-filename ui-1heor9g md-inline-path-filename">metrics.queue_length</span>` breaks; use `metrics["queue_length"]`.
* `close()` is required on `PersistBase` subclasses.

**Handlers (breaking only if you coded to the old object, not the docs)**

* Process now passes `{"data": ..., "meta": ...}`. Handlers that did `message.message["data"]` or used `<span class="ui-markdown__inline-path-filename ui-1heor9g md-inline-path-filename">message.msg_id</span>` will fail. Handlers that already used `message["data"]` as documented start working.
* `handle` always runs, even when `validate` is missing. That was a bug; anything that relied on “no validate ⇒ skip handle” would change.

**Behavior / CLI**

* An empty queue **stops** instead of spinning until `--max-runtime`. Jobs that expected the process to stay up for the full window will return early.
* Sample handlers moved out of the installed package (`<span class="ui-markdown__inline-path-filename ui-1heor9g md-inline-path-filename">msg_bus.handlers.mb_test</span>` / `exception_test` are gone).
* Helpers such as `handle_message` / `validate_message` / `get_handlers` now live on `<span class="ui-markdown__inline-path-filename ui-1heor9g md-inline-path-filename">msg_bus.processor</span>`, not `<span class="ui-markdown__inline-path-filename ui-1heor9g md-inline-path-filename">msg_bus.cli.process</span>`.
* Invalid `--action` on `msg-bus-queue` is a Click choice error now, not the old “Invalid action: …” text.
* DSN parsing is stricter: missing/invalid DSNs raise; hostless URLs like `postgres:///db` are rejected when actually connecting.

**Not breaking**

* Enqueue CLI meta flags, public `__init__` exports, optional `validate` on `BaseHandler`, and dropping icecream/urllib3.
