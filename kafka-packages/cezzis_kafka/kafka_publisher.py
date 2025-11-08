
import logging
from typing import Callable, Optional, Dict, Any
from confluent_kafka import Producer
import time
import uuid

from cezzis_kafka.delivery_handler import DeliveryHandler

logger = logging.getLogger(__name__)

class KafkaPublisher:
    """Enterprise Kafka publisher with robust delivery handling."""
    
    def __init__(
        self, 
        broker_url: str,
        max_retries: int = 3,
        dlq_topic: Optional[str] = None,
        metrics_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        producer_config: Optional[Dict[str, Any]] = None
    ):
        self.broker_url = broker_url
        
        # Default producer configuration
        config = {
            'bootstrap.servers': broker_url,
            'acks': 'all',  # Wait for all replicas
            'retries': 0,   # We handle retries in delivery callback
            'max.in.flight.requests.per.connection': 1,  # Ensure ordering
            'enable.idempotence': True,  # Prevent duplicates
            'compression.type': 'snappy',  # Efficient compression
        }
        
        # Override with user config
        if producer_config:
            config.update(producer_config)
        
        self._producer = Producer(config)
        self._delivery_handler = DeliveryHandler(
            max_retries=max_retries,
            dlq_topic=dlq_topic,
            metrics_callback=metrics_callback
        )

    def send(
        self,
        topic: str,
        message: str | bytes,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str | bytes]] = None,
        message_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
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
        
        # Ensure headers include message ID
        final_headers = {**(headers or {})}
        final_headers['message_id'] = message_id
        
        # Register for delivery tracking
        self._delivery_handler.track_message(message_id, topic, metadata or {})
        
        try:
            self._producer.produce(
                topic=topic,
                value=message,
                key=key,
                headers=final_headers,
                on_delivery=self._delivery_handler.handle_delivery
            )
            
            logger.info(
                "Message queued for delivery",
                extra={
                    "message_id": message_id,
                    "topic": topic,
                    "key": key,
                    **({} if not metadata else metadata)
                }
            )
            
            return message_id
            
        except Exception as e:
            # Remove from tracking on immediate failure
            self._delivery_handler.remove_pending_message(message_id)
            
            logger.error(
                "Failed to queue message for delivery",
                exc_info=True,
                extra={
                    "message_id": message_id,
                    "topic": topic,
                    "error": str(e)
                }
            )
            raise
    
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
        self.flush()


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
        