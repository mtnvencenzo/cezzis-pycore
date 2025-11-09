import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class DeliveryContext:
    """Context information for message delivery tracking.

    Attributes:
        message_id (str): Unique identifier for the message.
        topic (str): Kafka topic to which the message was sent.
        attempt_count (int): Number of delivery attempts made.
        original_timestamp (float): Timestamp when the message was originally sent.
        metadata (Dict[str, Any]): User-provided metadata (sanitized copy).
        original_message_snapshot (Optional[Dict[str, Any]]): Internal snapshot of original message data for retries.
    """

    message_id: str
    topic: str
    attempt_count: int = 0  # Start at 0, incremented on each retry attempt
    original_timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    original_message_snapshot: Optional[Dict[str, Any]] = None
