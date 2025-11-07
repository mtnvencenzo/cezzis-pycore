"""Cezzis Kafka - A lightweight library for Apache Kafka message consumption."""

from cezzis_kafka.ikafka_message_processor import IKafkaMessageProcessor
from cezzis_kafka.kafka_consumer import spawn_consumers, start_consumer
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings

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
]
