# Message Bus Queues

The initial version uses the PostgreSQL/postgres pgmq extention and the related python pgmq package to manage the message queue. It implements an interface so chaning out the back end queue system should be fairly straight forward. Where options would apply to different back ends options dict[str,any] is used to allow flexibility. This code is basicly a wrapper around python pgmq-py with structured meta being added to the message. The message is split into data and meta. {"data": {}, "meta": {}} to manage specific needs.

## Depends On

This project uses several packages but the true core of the queue system is pgmq. 

- [Postgres pgmq extension](https://github.com/pgmq/pgmq)
- [Python pgmq-py package](https://github.com/pgmq/pgmq-py)

## DSN

You will need to export the DSN or use the --dsn parameter if using cli tools.

```shell
PGMQ_DSN=postgresql://msg_bus:PASSWORD@localhost:5432/pgmq_d
```

## Message Data

Data can be any serializable data the handleer may need. 

## Message Meta

I went very light on the meta data. I add queue_name for simplicity of lookup, the error and stack_trace to assist with error tracking. These should be mostly self explanitory. The following items possibly need a little more clarification. 

### Correlation ID

This is the ID for the originating topic, think "employee hired". If fanout the process will need a way to know when all tasks are completed. This is the ID that allows your process to verify that the tasks are completed. May or may not be needed in your implementation but, I'll need it so added.

### Target ID

The target ID used by the task.  i.e. The ID of the object to be acted upon. At the task level the data needed should be provided by the process that adds the queue item. This is not intended for looking up additional data in the handler! This is purely to allow the tracking of actions taken on a target across the multiple queues and allowing the building of a more holistic view into the chain of events that impact target. 

### Version

The version of the message. Intended to be used for handlers to know how to route the message data while migrating message data formats. When if you need it it's here. 

## Command Line Tools (CLI)

You can trigger these with `uv run tool --help` replacing tool with the listed tool name below to get help and understand the parameters. 

- **msg-bus-enqueue** Adds an item to a queue.
- **msg-bus-status** Lists basic metrics about a queue
- **msg-bus-process** Handles the messages in a queue

### Handling Messages

There is a bit to this, but the basics are that you run the process CLI tool with appropriate parameters and for every queue you process you have an identically named module in handlers. i.e. exception_test.py is the handler for the exception_test queue. Look at the base class or provided handlers. 

