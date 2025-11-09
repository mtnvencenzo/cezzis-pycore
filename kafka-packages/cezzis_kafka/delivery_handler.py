import logging
import time
from typing import Any, Callable, Dict, Optional

from confluent_kafka import KafkaError, Message

from cezzis_kafka.delivery_context import DeliveryContext
from cezzis_kafka.delivery_status import DeliveryStatus

logger = logging.getLogger(__name__)


class DeliveryHandler:
    """Enterprise-level delivery callback handler for metrics and monitoring."""

    def __init__(
        self,
        metrics_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        """Initialize the DeliveryHandler.

        Args:
            metrics_callback (Optional[Callable[[str, Dict[str, Any]], None]]): Callback for reporting metrics.

        Returns:
            None

        """
        self.metrics_callback = metrics_callback
        self._pending_messages: Dict[str, DeliveryContext] = {}

    def track_message(
        self,
        message_id: str,
        topic: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a message for delivery tracking.

        Args:
            message_id (str): Unique identifier for the message.
            topic (str): Kafka topic name.
            metadata (Optional[Dict[str, Any]]): Additional metadata for tracking.

        Returns:
            None
        """
        # Create a shallow copy of metadata to avoid mutating caller's data
        sanitized_metadata = (metadata or {}).copy()

        context = DeliveryContext(message_id=message_id, topic=topic, metadata=sanitized_metadata)
        self._pending_messages[message_id] = context

    def handle_delivery(self, err: KafkaError | None, msg: Message) -> None:
        """
        Delivery callback that handles success and failure reporting.

        Args:
            err: The KafkaError if delivery failed, else None
            msg: The Kafka message that was delivered or failed
        """
        # Extract message identifier (from headers or key)
        message_id = self._extract_message_id(msg)
        context = self._pending_messages.pop(message_id, None)

        if not context:
            logger.warning(
                "Received delivery report for unknown message", extra={"message_id": message_id, "topic": msg.topic()}
            )
            return

        try:
            if err is None:
                self._handle_successful_delivery(context, msg)
            else:
                self._handle_failed_delivery(context, msg, err)
        except Exception as e:
            logger.error(
                "Error in delivery callback",
                exc_info=True,
                extra={
                    "message_id": context.message_id,
                    "topic": msg.topic(),
                    "callback_error": str(e),
                },
            )

    def _extract_message_id(self, msg: Message) -> str:
        """Extract message ID from message headers or key.

        Args:
            msg (Message): The Kafka message.

        Returns:
            str: Extracted message ID.
        """
        headers = msg.headers()
        if headers:
            for key, value in headers:
                if key == "message_id":
                    return value.decode("utf-8") if isinstance(value, bytes) else str(value)

        # Fallback to message key or generate one
        msg_key = msg.key()
        if msg_key:
            return msg_key.decode("utf-8")

        return f"{msg.topic()}_{msg.partition()}_{msg.offset()}"

    def _handle_successful_delivery(self, context: DeliveryContext, msg: Message) -> None:
        """Handle successful message delivery.

        Args:
            context (DeliveryContext): Context of the delivered message.
            msg (Message): The Kafka message that was delivered.

        Returns:
            None
        """
        delivery_time = time.time() - context.original_timestamp

        logger.info(
            "Message delivered successfully",
            extra={
                "message_id": context.message_id,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "delivery_time_ms": delivery_time * 1000,
                **context.metadata,
            },
        )

        # Report success metrics
        if self.metrics_callback:
            self.metrics_callback(
                "kafka.message.delivered",
                {
                    "status": DeliveryStatus.SUCCESS.value,
                    "topic": msg.topic(),
                    "delivery_time_ms": delivery_time * 1000,
                    "message_id": context.message_id,
                },
            )

    def _handle_failed_delivery(self, context: DeliveryContext, msg: Message, err: KafkaError) -> None:
        """Handle failed message delivery.

        Note: Since we're using Kafka's built-in retries, this is the final failure
        after all retry attempts have been exhausted.

        Args:
            context (DeliveryContext): Context of the failed message.
            msg (Message): The Kafka message that failed to deliver.
            err (KafkaError): The error that occurred during delivery.

        Returns:
            None
        """
        status = self._classify_error(err)

        logger.error(
            "Message delivery failed after all retries",
            extra={
                "message_id": context.message_id,
                "topic": msg.topic(),
                "error_code": err.code(),
                "error_message": str(err),
                "status": status.value,
                **context.metadata,
            },
        )

        # Report failure metrics
        if self.metrics_callback:
            self.metrics_callback(
                "kafka.message.failed",
                {
                    "status": status.value,
                    "topic": msg.topic(),
                    "error_code": err.code(),
                    "message_id": context.message_id,
                },
            )

    def _classify_error(self, err: KafkaError) -> DeliveryStatus:
        """
        Classify Kafka errors for monitoring and metrics.

        Args:
            err: The KafkaError encountered during delivery.

        Returns:
            DeliveryStatus indicating the type of error
        """
        # Network/broker temporary issues
        if err.code() in [
            KafkaError.NETWORK_EXCEPTION,
            KafkaError.BROKER_NOT_AVAILABLE,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError.REQUEST_TIMED_OUT,
            KafkaError.NOT_ENOUGH_REPLICAS,
            KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            KafkaError.THROTTLING_QUOTA_EXCEEDED,
        ]:
            return DeliveryStatus.RETRIABLE_ERROR

        # Configuration/authorization errors
        if err.code() in [
            KafkaError.TOPIC_AUTHORIZATION_FAILED,
            KafkaError.CLUSTER_AUTHORIZATION_FAILED,
            KafkaError.INVALID_CONFIG,
            KafkaError.UNKNOWN_TOPIC_OR_PART,
            KafkaError.MSG_SIZE_TOO_LARGE,
            KafkaError.INVALID_MSG_SIZE,
            KafkaError.INVALID_MSG,
        ]:
            return DeliveryStatus.FATAL_ERROR

        # Timeout errors (delivery timeout exceeded)
        if err.code() in [KafkaError._MSG_TIMED_OUT]:
            return DeliveryStatus.TIMEOUT

        # Default to retriable for unknown errors
        return DeliveryStatus.RETRIABLE_ERROR

    def remove_pending_message(self, message_id: str) -> None:
        """Remove a message from pending tracking.

        Args:
            message_id (str): Unique identifier for the message.
        """
        self._pending_messages.pop(message_id, None)

    def close(self) -> None:
        """Clean up resources and clear pending messages."""
        logger.info("Shutting down delivery handler")

        # Clear pending messages
        self._pending_messages.clear()
        logger.info("Delivery handler shutdown complete")
