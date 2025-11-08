from dataclasses import dataclass, field
import time
from typing import Any, Dict

@dataclass
class DeliveryContext:
    """Context information for message delivery tracking."""
    message_id: str
    topic: str
    attempt_count: int = 1
    original_timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)