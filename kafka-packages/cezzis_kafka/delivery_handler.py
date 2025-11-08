import logging
import time
from typing import Any, Callable, Dict, Optional
from confluent_kafka import KafkaError, Message
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
        metrics_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
    ):
        """Initialize the DeliveryHandler.
        
        Args:
            max_retries (int): Maximum number of retries for retriable errors.
            retry_backoff_ms (int): Backoff time in milliseconds between retries.
            dlq_topic (Optional[str]): Dead Letter Queue topic name for terminal failures.
            metrics_callback (Optional[Callable[[str, Dict[str, Any]], None]]): Callback for reporting metrics.

        Returns:
            None
        
        """

        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.dlq_topic = dlq_topic
        self.metrics_callback = metrics_callback
        self._pending_messages: Dict[str, DeliveryContext] = {}
    
    def track_message(self, message_id: str, topic: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Register a message for delivery tracking.
        
        Args:
            message_id (str): Unique identifier for the message.
            topic (str): Kafka topic name.
            metadata (Optional[Dict[str, Any]]): Additional metadata for tracking.

        Returns:
            None
        """
        self._pending_messages[message_id] = DeliveryContext(
            message_id=message_id,
            topic=topic,
            metadata=metadata or {}
        )
    
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
                "Received delivery report for unknown message",
                extra={"message_id": message_id, "topic": msg.topic()}
            )
            return
        
        try:
            if err is None:
                self._handle_successful_delivery(context, msg)
            else:
                self._handle_failed_delivery(context, msg, err)
        finally:
            # Clean up tracking for successful deliveries or exhausted retries
            if err is None or context.attempt_count >= self.max_retries:
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
                if key == 'message_id':
                    return value.decode('utf-8') if isinstance(value, bytes) else str(value)
        
        # Fallback to message key or generate one
        msg_key = msg.key()
        if msg_key:
            return msg_key.decode('utf-8')
        
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
                **context.metadata
            }
        )
        
        # Report success metrics
        if self.metrics_callback:
            self.metrics_callback("kafka.message.delivered", {
                "status": DeliveryStatus.SUCCESS.value,
                "topic": msg.topic(),
                "attempt_count": context.attempt_count,
                "delivery_time_ms": delivery_time * 1000
            })
    
    def _handle_failed_delivery(self, context: DeliveryContext, msg: Message, err: KafkaError) -> None:
        """Handle failed message delivery with retry logic.
        
        Args:
            context (DeliveryContext): Context of the failed message.
            msg (Message): The Kafka message that failed to deliver.
            err (KafkaError): The error that occurred during delivery.

        Returns:
            None
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
                **context.metadata
            }
        )
        
        # Report failure metrics
        if self.metrics_callback:
            self.metrics_callback("kafka.message.failed", {
                "status": status.value,
                "topic": msg.topic(),
                "attempt_count": context.attempt_count,
                "error_code": err.code()
            })
        
        # Handle based on error classification
        if status == DeliveryStatus.RETRIABLE_ERROR and context.attempt_count < self.max_retries:
            self._schedule_retry(context, msg)
        else:
            self._handle_terminal_failure(context, msg, err, status)
    
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
        """Schedule message for retry (in real implementation, use a proper scheduler).
        
        Args:
            context (DeliveryContext): Context of the message to retry.
            msg (Message): The Kafka message to retry.

        Returns:
            None
        """
        context.attempt_count += 1
        
        logger.info(
            "Scheduling message retry",
            extra={
                "message_id": context.message_id,
                "topic": msg.topic(),
                "attempt_count": context.attempt_count,
                "max_retries": self.max_retries,
                "backoff_ms": self.retry_backoff_ms * context.attempt_count
            }
        )
        
        # In production, you'd use a proper retry mechanism:
        # - Redis/database queue with delayed execution
        # - Separate retry topic with time-based processing
        # - In-memory scheduler with exponential backoff
        # For now, we just log the retry intent
    
    def _handle_terminal_failure(self, context: DeliveryContext, msg: Message, err: KafkaError, status: DeliveryStatus) -> None:
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
                "dlq_topic": self.dlq_topic
            }
        )
        
        # Send to Dead Letter Queue if configured
        if self.dlq_topic:
            self._send_to_dlq(context, msg, err)
        
        # Report terminal failure metrics
        if self.metrics_callback:
            self.metrics_callback("kafka.message.terminal_failure", {
                "status": status.value,
                "topic": msg.topic(),
                "final_attempt_count": context.attempt_count,
                "sent_to_dlq": self.dlq_topic is not None
            })
    
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
            return value.decode('utf-8', errors='replace')
        
        return str(value)
    
    def _send_to_dlq(self, context: DeliveryContext, original_msg: Message, err: KafkaError) -> None:
        """Send failed message to Dead Letter Queue with error context.
        
        Args:
            context (DeliveryContext): Context of the failed message.
            original_msg (Message): The original Kafka message that failed.
            err (KafkaError): The error that occurred during delivery.

        Returns:
            None
        """
        dlq_payload = {
            "original_topic": original_msg.topic(),
            "original_partition": original_msg.partition(),
            "message_id": context.message_id,
            "failure_reason": str(err),
            "error_code": err.code(),
            "attempt_count": context.attempt_count,
            "original_timestamp": context.original_timestamp,
            "failure_timestamp": time.time(),
            "original_value": self._safe_decode_value(original_msg.value()),
            "metadata": context.metadata
        }
        
        logger.info(
            "Sending message to DLQ",
            extra={
                "message_id": context.message_id,
                "original_topic": original_msg.topic(),
                "dlq_topic": self.dlq_topic
            }
        )
        
        dlq_payload = {
            "original_topic": original_msg.topic(),
            "original_partition": original_msg.partition(),
            "message_id": context.message_id,
            "failure_reason": str(err),
            "error_code": err.code(),
            "attempt_count": context.attempt_count,
            "original_timestamp": context.original_timestamp,
            "failure_timestamp": time.time(),
            "original_value": self._safe_decode_value(original_msg.value()),
            "metadata": context.metadata
        }
        
        logger.info(
            "Sending message to DLQ",
            extra={
                "message_id": context.message_id,
                "original_topic": original_msg.topic(),
                "dlq_topic": self.dlq_topic
            }
        )
        
        # In production, you'd send this to the DLQ topic
        # For now, we just log the DLQ payload
        logger.debug("DLQ payload", extra={"dlq_payload": dlq_payload})