"""Cezzis Kafka - A lightweight library for Apache Kafka message consumption."""

from cezzis_kafka.delivery_context import DeliveryContext
from cezzis_kafka.delivery_handler import DeliveryHandler
from cezzis_kafka.delivery_status import DeliveryStatus
from cezzis_kafka.ikafka_message_processor import IKafkaMessageProcessor
from cezzis_kafka.kafka_consumer import spawn_consumers, start_consumer
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings
from cezzis_kafka.kafka_publisher import KafkaPublisher
from cezzis_kafka.kafka_publisher_settings import KafkaPublisherSettings

# Dynamically read version from package metadata
try:
    from importlib.metadata import version

    __version__ = version("cezzis_kafka")
except Exception:
    __version__ = "unknown"

__all__ = [
    "IKafkaMessageProcessor",
    "KafkaConsumerSettings",
    "start_consumer",
    "spawn_consumers",
    "KafkaPublisherSettings",
    "KafkaPublisher",
    "DeliveryStatus",
    "DeliveryContext",
    "DeliveryHandler",
]
