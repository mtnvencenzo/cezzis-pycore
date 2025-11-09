import logging
import time
import uuid
from typing import Any, Dict, Optional

from confluent_kafka import Producer

from cezzis_kafka.kafka_producer_settings import KafkaProducerSettings

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Enterprise Kafka producer with robust delivery handling."""

    def __init__(self, settings: KafkaProducerSettings):
        """Initialize the KafkaProducer with settings.

        Args:
            settings (KafkaProducerSettings): Configuration settings for the producer.
        """
        self.settings = settings

        # Default producer configuration
        config = {
            "bootstrap.servers": settings.bootstrap_servers,
            "acks": "all",
            "retries": settings.max_retries,
            "retry.backoff.ms": settings.retry_backoff_ms,
            "retry.backoff.max.ms": settings.retry_backoff_max_ms,
            "delivery.timeout.ms": settings.delivery_timeout_ms,
            "request.timeout.ms": settings.request_timeout_ms,
            "max.in.flight.requests.per.connection": 5,  # Allow more concurrent requests
            "enable.idempotence": True,
            "compression.type": "snappy",
        }

        # Override with user config from settings
        config.update(settings.producer_config)

        self._producer = Producer(config)

    @property
    def broker_url(self) -> str:
        """Get the broker URL for backward compatibility."""
        return self.settings.bootstrap_servers

    def send(
        self,
        topic: str,
        message: str | bytes,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str | bytes]] = None,
        message_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Send a message to Kafka with enterprise-level delivery tracking.

        Args:
            topic: Kafka topic name
            message: Message payload
            key: Optional message key for partitioning
            headers: Optional message headers
            message_id: Optional unique identifier (generated if not provided)
            metadata: Optional metadata for tracking/metrics

        Returns:
            str: Message ID for tracking
        """
        # Generate message ID if not provided
        if not message_id:
            message_id = self._generate_message_id(topic)

        # Prepare headers with message ID
        final_headers = {**(headers or {})}
        final_headers["message_id"] = message_id

        try:
            self._producer.produce(
                topic=topic,
                value=message,
                key=key,
                headers=final_headers,
                on_delivery=self.settings.on_delivery,
            )

            logger.info(
                "Message queued for delivery",
                extra={"message_id": message_id, "topic": topic, "key": key, **({} if not metadata else metadata)},
            )

            return message_id

        except Exception as e:
            logger.error(
                "Failed to queue message for delivery",
                exc_info=True,
                extra={"message_id": message_id, "topic": topic, "error": str(e)},
            )
            raise

    def send_and_wait(
        self,
        topic: str,
        message: str | bytes,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str | bytes]] = None,
        message_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> str:
        """
        Send a message and wait for confirmation of delivery.

        Args:
            topic: Kafka topic name
            message: Message payload
            key: Optional message key for partitioning
            headers: Optional message headers
            message_id: Optional unique identifier (generated if not provided)
            metadata: Optional metadata for tracking/metrics
            timeout: Maximum time to wait for delivery confirmation

        Returns:
            str: Message ID for tracking

        Raises:
            Exception: If message delivery fails or times out
        """
        message_id = self.send(topic, message, key, headers, message_id, metadata)

        # Poll to process any immediate callbacks
        self.poll(0.1)

        # Wait for delivery with appropriate timeout
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            raise Exception(
                f"Failed to deliver message within {timeout}s timeout. {remaining} messages remain in queue."
            )

        return message_id

    def flush(self, timeout: float = 10.0) -> None:
        """Wait for all messages to be delivered or fail.

        Args:
            timeout (float): Maximum time to wait for delivery in seconds.

        Returns:
            None
        """

        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"Failed to deliver {remaining} messages within timeout")

    def close(self) -> None:
        """Close the producer and ensure all messages are delivered."""
        # First flush any pending messages
        self.flush()

        logger.info("Kafka producer closed successfully")

    def poll(self, timeout: float = 0) -> int:
        """Poll for delivery events and trigger callbacks.

        Args:
            timeout: Maximum time to wait for events in seconds

        Returns:
            Number of events processed
        """
        return self._producer.poll(timeout)

    def get_queue_size(self) -> int:
        """Get the current number of messages in the producer queue.

        Returns:
            int: Number of messages waiting to be sent
        """
        return len(self._producer)

    def _generate_message_id(self, topic: str) -> str:
        """
        Generate a unique message ID suitable for high-load production environments.  Uses timestamp + random UUID suffix for uniqueness with high performance.

        Args:
            topic (str): Kafka topic name.

        Returns:
            str: Generated unique message ID.
        """
        timestamp_ms = int(time.time() * 1000)
        random_suffix = uuid.uuid4().hex[:8]

        return f"{topic}_{timestamp_ms}_{random_suffix}"
