import json
import logging
import threading
import time
from typing import Any, Callable, Dict, Optional

from confluent_kafka import KafkaError, Message, Producer

from cezzis_kafka.delivery_context import DeliveryContext
from cezzis_kafka.delivery_status import DeliveryStatus

logger = logging.getLogger(__name__)


class DeliveryHandler:
    """Enterprise-level delivery callback handler with retry logic and DLQ support."""

    def __init__(
        self,
        max_retries: int = 3,
        retry_backoff_ms: int = 1000,
        dlq_topic: Optional[str] = None,
        metrics_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        bootstrap_servers: Optional[str] = None,
        retry_producer: Optional[Producer] = None,
    ):
        """Initialize the DeliveryHandler.

        Args:
            max_retries (int): Maximum number of retries for retriable errors.
            retry_backoff_ms (int): Backoff time in milliseconds between retries.
            dlq_topic (Optional[str]): Dead Letter Queue topic name for terminal failures.
            metrics_callback (Optional[Callable[[str, Dict[str, Any]], None]]): Callback for reporting metrics.
            bootstrap_servers (Optional[str]): Kafka bootstrap servers for DLQ producer.
            retry_producer (Optional[Producer]): Producer instance to use for retries.

        Returns:
            None

        """

        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.dlq_topic = dlq_topic
        self.metrics_callback = metrics_callback
        self._pending_messages: Dict[str, DeliveryContext] = {}
        self._retry_producer = retry_producer  # Producer for retry attempts
        self._retry_timers: Dict[str, threading.Timer] = {}  # Track active retry timers
        self._shutdown = False  # Flag to prevent new retries during shutdown

        # Initialize DLQ producer if DLQ topic is configured
        self._dlq_producer: Optional[Producer] = None
        if dlq_topic and bootstrap_servers:
            self._init_dlq_producer(bootstrap_servers)

    def _init_dlq_producer(self, bootstrap_servers: str) -> None:
        """Initialize the DLQ producer with optimized settings for reliability."""
        dlq_config = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",  # Ensure DLQ messages are reliably stored
            "retries": 5,  # Retry DLQ sends (more than main producer)
            "retry.backoff.ms": 1000,
            "max.in.flight.requests.per.connection": 1,
            "enable.idempotence": True,
            "compression.type": "snappy",
            "request.timeout.ms": 30000,  # Longer timeout for DLQ
            "delivery.timeout.ms": 60000,  # Ensure DLQ delivery
        }

        try:
            self._dlq_producer = Producer(dlq_config)
            logger.info(
                "DLQ producer initialized successfully",
                extra={"dlq_topic": self.dlq_topic, "bootstrap_servers": bootstrap_servers},
            )
        except Exception as e:
            logger.error(
                "Failed to initialize DLQ producer", exc_info=True, extra={"dlq_topic": self.dlq_topic, "error": str(e)}
            )
            # Continue without DLQ producer - we'll log errors instead
            self._dlq_producer = None

    def track_message(
        self,
        message_id: str,
        topic: str,
        metadata: Optional[Dict[str, Any]] = None,
        original_message_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a message for delivery tracking.

        Args:
            message_id (str): Unique identifier for the message.
            topic (str): Kafka topic name.
            metadata (Optional[Dict[str, Any]]): Additional metadata for tracking.
            original_message_data (Optional[Dict[str, Any]]): Original message data for retries.

        Returns:
            None
        """
        context = DeliveryContext(message_id=message_id, topic=topic, metadata=metadata or {})

        # Store original message data for retry attempts
        if original_message_data:
            context.metadata["_original_message"] = original_message_data

        self._pending_messages[message_id] = context

    def handle_delivery(self, err: KafkaError | None, msg: Message) -> None:
        """
        Delivery callback that handles success, retries, and DLQ routing.

        Args:
            err: The KafkaError if delivery failed, else None
            msg: The Kafka message that was delivered or failed
        """
        # Extract message identifier (from headers or key)
        message_id = self._extract_message_id(msg)
        context = self._pending_messages.get(message_id)

        if not context:
            logger.warning(
                "Received delivery report for unknown message", extra={"message_id": message_id, "topic": msg.topic()}
            )
            return

        try:
            if err is None:
                self._handle_successful_delivery(context, msg)
            else:
                should_cleanup = self._handle_failed_delivery(context, msg, err)
                if should_cleanup:
                    self._pending_messages.pop(message_id, None)
        finally:
            # Clean up tracking for successful deliveries
            if err is None:
                self._pending_messages.pop(message_id, None)

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
                "attempt_count": context.attempt_count,
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
                    "attempt_count": context.attempt_count,
                    "delivery_time_ms": delivery_time * 1000,
                },
            )

    def _handle_failed_delivery(self, context: DeliveryContext, msg: Message, err: KafkaError) -> bool:
        """Handle failed message delivery with retry logic.

        Args:
            context (DeliveryContext): Context of the failed message.
            msg (Message): The Kafka message that failed to deliver.
            err (KafkaError): The error that occurred during delivery.

        Returns:
            bool: True if message should be cleaned up (no more retries), False otherwise
        """
        status = self._classify_error(err)

        logger.error(
            "Message delivery failed",
            extra={
                "message_id": context.message_id,
                "topic": msg.topic(),
                "attempt_count": context.attempt_count,
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
                    "attempt_count": context.attempt_count,
                    "error_code": err.code(),
                },
            )

        # Handle based on error classification
        if status == DeliveryStatus.RETRIABLE_ERROR and context.attempt_count < self.max_retries:
            self._schedule_retry(context, msg)
            return False  # Keep tracking, retry scheduled
        else:
            self._handle_terminal_failure(context, msg, err, status)
            return True  # Clean up tracking, terminal failure

    def _classify_error(self, err: KafkaError) -> DeliveryStatus:
        """
        Classify Kafka errors to determine retry strategy.

        Args:
            err: The KafkaError encountered during delivery.

        Returns:
            DeliveryStatus indicating how to handle the error
        """
        # Network/broker temporary issues - retriable
        if err.code() in [
            KafkaError.NETWORK_EXCEPTION,
            KafkaError.BROKER_NOT_AVAILABLE,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError.REQUEST_TIMED_OUT,
            KafkaError.NOT_ENOUGH_REPLICAS,
            KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        ]:
            return DeliveryStatus.RETRIABLE_ERROR

        # Quota/throttling issues - retriable
        if err.code() in [
            KafkaError.THROTTLING_QUOTA_EXCEEDED,
        ]:
            return DeliveryStatus.RETRIABLE_ERROR

        # Configuration/authorization errors - fatal
        if err.code() in [
            KafkaError.TOPIC_AUTHORIZATION_FAILED,
            KafkaError.CLUSTER_AUTHORIZATION_FAILED,
            KafkaError.INVALID_CONFIG,
            KafkaError.UNKNOWN_TOPIC_OR_PART,
        ]:
            return DeliveryStatus.FATAL_ERROR

        # Message size/format errors - fatal
        if err.code() in [
            KafkaError.MSG_SIZE_TOO_LARGE,
            KafkaError.INVALID_MSG_SIZE,
            KafkaError.INVALID_MSG,
        ]:
            return DeliveryStatus.FATAL_ERROR

        # Default to retriable for unknown errors
        return DeliveryStatus.RETRIABLE_ERROR

    def _schedule_retry(self, context: DeliveryContext, msg: Message) -> None:
        """Schedule message for retry with exponential backoff.

        Args:
            context (DeliveryContext): Context of the message to retry.
            msg (Message): The Kafka message to retry.

        Returns:
            None
        """
        if self._shutdown:
            logger.warning(
                "Cannot schedule retry - handler is shutting down",
                extra={"message_id": context.message_id, "topic": msg.topic()},
            )
            return

        if not self._retry_producer:
            logger.error(
                "Cannot retry message - no retry producer configured",
                extra={"message_id": context.message_id, "topic": msg.topic()},
            )
            return

        context.attempt_count += 1

        # Calculate exponential backoff: base_delay * (2 ^ attempt - 1) with jitter
        backoff_factor = 2 ** (context.attempt_count - 1)
        backoff_ms = self.retry_backoff_ms * backoff_factor

        # Add jitter (±25%) to prevent thundering herd
        import random

        jitter = random.uniform(0.75, 1.25)
        final_backoff_ms = backoff_ms * jitter

        logger.info(
            "Scheduling message retry with exponential backoff",
            extra={
                "message_id": context.message_id,
                "topic": msg.topic(),
                "attempt_count": context.attempt_count,
                "max_retries": self.max_retries,
                "backoff_ms": final_backoff_ms,
                "backoff_factor": backoff_factor,
            },
        )

        # Cancel any existing timer for this message
        existing_timer = self._retry_timers.get(context.message_id)
        if existing_timer:
            existing_timer.cancel()

        # Schedule the retry
        timer = threading.Timer(
            final_backoff_ms / 1000.0,  # Convert to seconds
            self._execute_retry,
            args=(context, msg),
        )

        self._retry_timers[context.message_id] = timer
        timer.start()

    def _execute_retry(self, context: DeliveryContext, original_msg: Message) -> None:
        """Execute the actual retry of a message.

        Args:
            context (DeliveryContext): Context of the message to retry.
            original_msg (Message): The original message that failed.

        Returns:
            None
        """
        # Remove the completed timer
        self._retry_timers.pop(context.message_id, None)

        if self._shutdown:
            logger.info("Skipping retry - handler is shutting down", extra={"message_id": context.message_id})
            return

        if not self._retry_producer:
            logger.error(
                "Cannot execute retry - no retry producer configured", extra={"message_id": context.message_id}
            )
            return

        try:
            # Extract original message data from context
            original_data = context.metadata.get("_original_message", {})
            topic = original_msg.topic()

            if not topic:
                raise ValueError("Original message has no topic")

            logger.info(
                "Executing message retry",
                extra={"message_id": context.message_id, "topic": topic, "attempt_count": context.attempt_count},
            )

            # Reproduce the message with retry headers
            retry_headers = original_data.get("headers", {}).copy()
            retry_headers.update(
                {
                    "retry_attempt": str(context.attempt_count),
                    "original_timestamp": str(context.original_timestamp),
                    "retry_timestamp": str(time.time()),
                }
            )

            # Send the retry message
            self._retry_producer.produce(
                topic=topic,
                key=original_data.get("key"),
                value=original_data.get("value"),
                headers=retry_headers,
                partition=original_data.get("partition"),
                on_delivery=self.handle_delivery,  # Use same delivery handler
            )

            # Report retry metrics
            if self.metrics_callback:
                self.metrics_callback(
                    "kafka.message.retry_attempted",
                    {"topic": topic, "attempt_count": context.attempt_count, "message_id": context.message_id},
                )

        except Exception as retry_err:
            logger.error(
                "Failed to execute message retry",
                exc_info=True,
                extra={
                    "message_id": context.message_id,
                    "topic": original_msg.topic(),
                    "attempt_count": context.attempt_count,
                    "retry_error": str(retry_err),
                },
            )

            # Treat retry execution failure as terminal
            self._handle_terminal_failure(
                context,
                original_msg,
                KafkaError(KafkaError.UNKNOWN, f"Retry execution failed: {retry_err}"),
                DeliveryStatus.FATAL_ERROR,
            )

    def _handle_terminal_failure(
        self, context: DeliveryContext, msg: Message, err: KafkaError, status: DeliveryStatus
    ) -> None:
        """Handle messages that cannot be retried.

        Args:
            context (DeliveryContext): Context of the failed message.
            msg (Message): The Kafka message that failed to deliver.
            err (KafkaError): The error that occurred during delivery.
            status (DeliveryStatus): Classified delivery status.

        Returns:
            None
        """
        logger.error(
            "Message delivery terminally failed",
            extra={
                "message_id": context.message_id,
                "topic": msg.topic(),
                "attempt_count": context.attempt_count,
                "status": status.value,
                "error_code": err.code(),
                "dlq_topic": self.dlq_topic,
            },
        )

        # Send to Dead Letter Queue if configured
        if self.dlq_topic:
            self._send_to_dlq(context, msg, err)

        # Report terminal failure metrics
        if self.metrics_callback:
            self.metrics_callback(
                "kafka.message.terminal_failure",
                {
                    "status": status.value,
                    "topic": msg.topic(),
                    "final_attempt_count": context.attempt_count,
                    "sent_to_dlq": self.dlq_topic is not None,
                },
            )

    def remove_pending_message(self, message_id: str) -> None:
        """Remove a message from pending tracking.

        Args:
            message_id (str): Unique identifier for the message.
        """
        self._pending_messages.pop(message_id, None)

    def _safe_decode_value(self, value: Optional[bytes]) -> Optional[str]:
        """Safely decode message value.

        Args:
            value (Optional[bytes]): The message value in bytes.

        Returns:
            Optional[str]: Decoded string value or None.
        """

        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")

        return str(value)

    def _serialize_for_dlq(self, obj: Any) -> Any:
        """Recursively serialize objects for JSON serialization in DLQ."""
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        elif isinstance(obj, dict):
            return {k: self._serialize_for_dlq(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._serialize_for_dlq(item) for item in obj]
        else:
            return obj

    def _send_to_dlq(self, context: DeliveryContext, original_msg: Message, err: KafkaError) -> None:
        """Send failed message to Dead Letter Queue with error context.

        Args:
            context (DeliveryContext): Context of the failed message.
            original_msg (Message): The original Kafka message that failed.
            err (KafkaError): The error that occurred during delivery.

        Returns:
            None
        """
        if not self._dlq_producer or not self.dlq_topic:
            logger.warning(
                "DLQ not configured or producer unavailable, logging failed message",
                extra={
                    "message_id": context.message_id,
                    "original_topic": original_msg.topic(),
                    "failure_reason": str(err),
                },
            )
            return

        # Create comprehensive DLQ payload
        offset = original_msg.offset()
        headers = original_msg.headers()
        key = original_msg.key()

        dlq_payload = {
            "original_topic": original_msg.topic(),
            "original_partition": original_msg.partition(),
            "original_offset": offset if offset and offset >= 0 else None,
            "message_id": context.message_id,
            "failure_reason": str(err),
            "error_code": err.code(),
            "attempt_count": context.attempt_count,
            "original_timestamp": context.original_timestamp,
            "failure_timestamp": time.time(),
            "original_headers": {
                k: v.decode("utf-8", errors="replace") if isinstance(v, bytes) else str(v) for k, v in headers
            }
            if headers
            else {},
            "original_key": self._safe_decode_value(key),
            "original_value": self._safe_decode_value(original_msg.value()),
            "metadata": self._serialize_for_dlq(context.metadata),
        }

        try:
            # Serialize payload to JSON
            dlq_message = json.dumps(dlq_payload, ensure_ascii=False)

            # Create DLQ headers with additional context
            dlq_headers = {
                "dlq.original_topic": original_msg.topic(),
                "dlq.message_id": context.message_id,
                "dlq.failure_code": str(err.code()),
                "dlq.failure_reason": str(err),
                "dlq.attempt_count": str(context.attempt_count),
                "dlq.timestamp": str(int(time.time() * 1000)),
            }

            # Send to DLQ with synchronous delivery for reliability
            self._dlq_producer.produce(
                topic=self.dlq_topic,
                key=context.message_id,  # Use message_id as key for partitioning
                value=dlq_message,
                headers=dlq_headers,
                on_delivery=self._dlq_delivery_callback,
            )

            # Flush immediately to ensure DLQ message is sent
            self._dlq_producer.flush(timeout=10.0)

            logger.info(
                "Message sent to DLQ successfully",
                extra={
                    "message_id": context.message_id,
                    "original_topic": original_msg.topic(),
                    "dlq_topic": self.dlq_topic,
                    "attempt_count": context.attempt_count,
                },
            )

        except Exception as dlq_err:
            logger.error(
                "Failed to send message to DLQ",
                exc_info=True,
                extra={
                    "message_id": context.message_id,
                    "original_topic": original_msg.topic(),
                    "dlq_topic": self.dlq_topic,
                    "dlq_error": str(dlq_err),
                    "original_failure": str(err),
                },
            )

    def _dlq_delivery_callback(self, err: Optional[KafkaError], msg: Message) -> None:
        """Callback for DLQ message delivery."""
        if err is not None:
            logger.error(
                "DLQ message delivery failed",
                extra={"dlq_topic": msg.topic() if msg else self.dlq_topic, "error": str(err)},
            )
        else:
            logger.debug(
                "DLQ message delivered successfully",
                extra={"dlq_topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()},
            )

    def close(self) -> None:
        """Close the DLQ producer, cancel retry timers, and clean up resources."""
        logger.info("Shutting down delivery handler")
        self._shutdown = True

        # Cancel all pending retry timers
        for message_id, timer in self._retry_timers.items():
            try:
                timer.cancel()
                logger.debug(f"Cancelled retry timer for message {message_id}")
            except Exception as e:
                logger.warning(f"Error cancelling retry timer for message {message_id}: {e}")

        self._retry_timers.clear()

        # Close DLQ producer
        if self._dlq_producer:
            try:
                # Flush any pending DLQ messages
                remaining = self._dlq_producer.flush(timeout=30.0)
                if remaining > 0:
                    logger.warning(f"Failed to flush {remaining} DLQ messages on close")

                logger.info("DLQ producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing DLQ producer: {e}")
            finally:
                self._dlq_producer = None

        # Clear pending messages
        self._pending_messages.clear()
        logger.info("Delivery handler shutdown complete")
