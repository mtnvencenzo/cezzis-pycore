"""Cezzis Kafka - A lightweight library for Apache Kafka message consumption."""

from cezzis_kafka.async_kafka_consumer import shutdown_consumers, spawn_consumers_async, start_consumer_async
from cezzis_kafka.iasync_kafka_message_processor import IAsyncKafkaMessageProcessor
from cezzis_kafka.ikafka_message_processor import IKafkaMessageProcessor
from cezzis_kafka.kafka_consumer import spawn_consumers, start_consumer
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings
from cezzis_kafka.kafka_producer import KafkaProducer
from cezzis_kafka.kafka_producer_settings import KafkaProducerSettings

# Dynamically read version from package metadata
try:
    from importlib.metadata import version

    __version__ = version("cezzis_kafka")
except Exception:
    __version__ = "unknown"

__all__ = [
    "KafkaConsumerSettings",
    "KafkaProducerSettings",
    "KafkaProducer",
    "IKafkaMessageProcessor",
    "start_consumer",
    "spawn_consumers",
    "IAsyncKafkaMessageProcessor",
    "start_consumer_async",
    "spawn_consumers_async",
    "shutdown_consumers",
]
