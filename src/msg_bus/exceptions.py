"""Public exceptions raised by the message bus."""


class DuplicateTargetError(Exception):
    """Raised when enqueue would duplicate a pending target on the same queue."""

    def __init__(self, queue_name: str, target_id: str, existing_msg_id: int) -> None:
        """Record the conflicting queue, target, and existing message id."""
        self.queue_name = queue_name
        self.target_id = target_id
        self.existing_msg_id = existing_msg_id
        super().__init__(
            f"Rejected: queue {queue_name!r} already has pending message {existing_msg_id} for target_id {target_id!r}"
        )
