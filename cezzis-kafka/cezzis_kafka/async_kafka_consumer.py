import asyncio
import json
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Type, TypeVar

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from cezzis_kafka.iasync_kafka_message_processor import IAsyncKafkaMessageProcessor
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings

logger = logging.getLogger(__name__)

TAsyncProcessor = TypeVar("TAsyncProcessor", bound=IAsyncKafkaMessageProcessor)
"""Type variable for IAsyncKafkaMessageProcessor or its subclasses."""

# Configurable thread pool size for production flexibility
_DEFAULT_THREAD_POOL_SIZE = int(os.getenv("KAFKA_ASYNC_THREAD_POOL_SIZE", "16"))
_DEFAULT_KAFKA_STATS_INTERVAL_MS = int(os.getenv("KAFKA_STATS_INTERVAL_MS", "10000"))

# Shared thread pool for all Kafka async operations - production optimization
# Sized to handle multiple consumer groups and topics efficiently
_kafka_thread_pool = ThreadPoolExecutor(max_workers=_DEFAULT_THREAD_POOL_SIZE, thread_name_prefix="kafka_async")


async def start_consumer_async(processor: IAsyncKafkaMessageProcessor) -> None:
    """Start async Kafka consumer with automatic reconnection on failure.

    If the consumer fails to connect or encounters an error during polling,
    it will automatically retry with exponential backoff. Reconnection settings
    are configured via KafkaConsumerSettings.

    Args:
        processor: Async message processor implementation
    """
    settings = processor.kafka_settings()
    attempt = 0

    while True:
        try:
            logger.info(
                "Starting async Kafka consumer...",
                extra={
                    "messaging.kafka.bootstrap_servers": settings.bootstrap_servers,
                    "messaging.kafka.consumer_group": settings.consumer_group,
                    "messaging.kafka.topic_name": settings.topic_name,
                    "messaging.kafka.reconnect_attempt": attempt,
                },
            )

            consumer = await _create_consumer_async(processor)
            if consumer is None:
                raise _ConsumerCreationError("Failed to create Kafka consumer")

            try:
                await _subscribe_consumer_async(consumer, processor)
                await _start_polling_async(consumer, processor)
            finally:
                await _close_consumer_async(consumer, processor)

        except asyncio.CancelledError:
            logger.info("Async consumer cancelled, shutting down...")
            raise

        except Exception as e:
            attempt += 1

            if settings.reconnect_max_retries > 0 and attempt >= settings.reconnect_max_retries:
                logger.error(
                    "Max reconnect retries reached, giving up",
                    extra={
                        "messaging.kafka.bootstrap_servers": settings.bootstrap_servers,
                        "messaging.kafka.consumer_group": settings.consumer_group,
                        "messaging.kafka.topic_name": settings.topic_name,
                        "messaging.kafka.reconnect_attempt": attempt,
                        "messaging.kafka.reconnect_max_retries": settings.reconnect_max_retries,
                        "error": str(e),
                    },
                )
                raise

            backoff = _calculate_backoff(
                attempt, settings.reconnect_backoff_seconds, settings.reconnect_backoff_max_seconds
            )

            logger.warning(
                "Kafka consumer failed, reconnecting after backoff",
                extra={
                    "messaging.kafka.bootstrap_servers": settings.bootstrap_servers,
                    "messaging.kafka.consumer_group": settings.consumer_group,
                    "messaging.kafka.topic_name": settings.topic_name,
                    "messaging.kafka.reconnect_attempt": attempt,
                    "messaging.kafka.reconnect_backoff_seconds": backoff,
                    "error": str(e),
                },
            )

            await asyncio.sleep(backoff)


class _ConsumerCreationError(Exception):
    """Internal error raised when consumer creation fails to trigger retry logic."""

    pass


def _calculate_backoff(attempt: int, base_seconds: float, max_seconds: float) -> float:
    """Calculate exponential backoff with jitter.

    Args:
        attempt: Current retry attempt number (1-based).
        base_seconds: Initial backoff in seconds.
        max_seconds: Maximum backoff cap in seconds.

    Returns:
        Backoff duration in seconds with jitter applied.
    """
    exponential = min(base_seconds * (2 ** (attempt - 1)), max_seconds)
    return random.uniform(exponential / 2, exponential)  # noqa: S311


async def _create_consumer_async(processor: IAsyncKafkaMessageProcessor) -> Optional[Consumer]:
    """Create Kafka consumer asynchronously."""

    await processor.consumer_creating()
    connected_brokers: set[str] = set()

    def _error_cb(err: KafkaError) -> None:
        """Callback for broker-level errors from confluent-kafka."""
        logger.error(
            "Kafka broker error",
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                "messaging.kafka.error_code": err.code(),
                "messaging.kafka.error_name": err.name(),
                "error": str(err),
            },
        )

    def _stats_cb(stats_json: str) -> None:
        """Callback for broker statistics from confluent-kafka."""
        try:
            stats = json.loads(stats_json)
        except json.JSONDecodeError:
            logger.debug("Failed to parse Kafka statistics payload", exc_info=True)
            return

        brokers = stats.get("brokers", {})
        if not isinstance(brokers, dict):
            return

        for broker_name, broker_stats in brokers.items():
            if not isinstance(broker_stats, dict):
                continue

            broker_state = broker_stats.get("state")

            if broker_state == "UP" and broker_name not in connected_brokers:
                connected_brokers.add(broker_name)
                logger.info(
                    "Kafka broker connection established",
                    extra={
                        "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                        "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                        "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                        "messaging.kafka.broker_name": broker_name,
                        "messaging.kafka.broker_state": broker_state,
                        "messaging.kafka.broker_node_id": broker_stats.get("nodeid"),
                    },
                )
            elif broker_state != "UP" and broker_name in connected_brokers:
                connected_brokers.remove(broker_name)

    def create_consumer():
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": processor.kafka_settings().bootstrap_servers,
                    "group.id": processor.kafka_settings().consumer_group,
                    "auto.offset.reset": processor.kafka_settings().auto_offset_reset,
                    "max.poll.interval.ms": processor.kafka_settings().max_poll_interval_ms,
                    "statistics.interval.ms": _DEFAULT_KAFKA_STATS_INTERVAL_MS,
                    "error_cb": _error_cb,
                    "stats_cb": _stats_cb,
                }
            )
            return consumer
        except Exception as e:
            logger.error(f"Failed to create consumer: {e}")
            return None

    # Run consumer creation in thread pool
    loop = asyncio.get_event_loop()
    consumer = await loop.run_in_executor(_kafka_thread_pool, create_consumer)

    await processor.consumer_created(consumer)

    if consumer:
        logger.info("Async Kafka consumer created successfully")

    return consumer


async def _subscribe_consumer_async(consumer: Consumer, processor: IAsyncKafkaMessageProcessor) -> None:
    """Subscribe consumer to topic asynchronously."""

    def subscribe():
        topic_name = processor.kafka_settings().topic_name
        consumer.subscribe([topic_name])
        logger.info(f"Consumer subscribed to topic: {topic_name}")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_kafka_thread_pool, subscribe)

    await processor.consumer_subscribed()


async def _start_polling_async(consumer: Consumer, processor: IAsyncKafkaMessageProcessor) -> None:
    """Start async polling loop for messages."""

    logger.info("Starting async polling for messages...")

    def poll_message(timeout: float = 1.0) -> Optional[Message]:
        """Poll for a single message."""
        return consumer.poll(timeout)

    loop = asyncio.get_event_loop()

    try:
        while True:
            # Check if task was cancelled
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                break

            # Poll for message in thread pool to avoid blocking event loop
            try:
                msg = await loop.run_in_executor(_kafka_thread_pool, poll_message, 1.0)
            except KafkaException as e:
                logger.error(
                    "Fatal Kafka error during poll, consumer will reconnect",
                    extra={
                        "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                        "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                        "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                        "error": str(e),
                    },
                )
                raise

            if msg is None:
                # No message received, continue polling
                await asyncio.sleep(0.01)  # Small delay to yield control
                continue

            if msg.error():
                error = msg.error()
                if error and error.code() == KafkaError._PARTITION_EOF:
                    await processor.message_partition_reached(msg)
                else:
                    logger.error(f"Consumer error: {error}")
                    await processor.message_error_received(msg)
            else:
                # Process message asynchronously
                await processor.message_received(msg)

    except asyncio.CancelledError:
        logger.info("Async polling cancelled")
        raise


async def _close_consumer_async(consumer: Consumer, processor: IAsyncKafkaMessageProcessor) -> None:
    """Close consumer asynchronously."""

    await processor.consumer_stopping()

    def close_consumer():
        try:
            consumer.commit()
            consumer.close()
            logger.info("Async consumer closed successfully")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_kafka_thread_pool, close_consumer)


async def spawn_consumers_async(
    factory_type: Type[TAsyncProcessor],
    num_consumers: int,
    bootstrap_servers: str,
    consumer_group: str,
    topic_name: str,
    max_poll_interval_ms: int = 300000,
    auto_offset_reset: str = "earliest",
) -> None:
    """Spawn multiple async Kafka consumers under a single consumer group.

    Args:
        factory_type (Type[TAsyncProcessor]): The factory type to create IAsyncKafkaMessageProcessor instances.
        num_consumers (int): The number of consumers to spawn.
        bootstrap_servers (str): The Kafka bootstrap servers.
        consumer_group (str): The consumer group ID.
        topic_name (str): The topic name to subscribe to.
        max_poll_interval_ms (int): Maximum poll interval in milliseconds. Defaults to 300000.
        auto_offset_reset (str): Auto offset reset policy. Defaults to "earliest".  Accepted values are "earliest", "latest", and "none".
    """

    logger.info(
        "Spawning async Kafka consumers",
        extra={
            "messaging.kafka.num_consumers": num_consumers,
            "messaging.kafka.bootstrap_servers": bootstrap_servers,
            "messaging.kafka.consumer_group": consumer_group,
            "messaging.kafka.topic_name": topic_name,
            "messaging.kafka.max_poll_interval_ms": max_poll_interval_ms,
            "messaging.kafka.auto_offset_reset": auto_offset_reset,
        },
    )

    # Create consumer tasks
    tasks = []
    for i in range(num_consumers):
        # Create processor instance using factory
        processor = factory_type.CreateNew(
            kafka_settings=KafkaConsumerSettings(
                bootstrap_servers=bootstrap_servers,
                consumer_group=consumer_group,
                topic_name=topic_name,
                num_consumers=num_consumers,
                max_poll_interval_ms=max_poll_interval_ms,
                auto_offset_reset=auto_offset_reset,
            )
        )

        # Create async task for each consumer
        task = asyncio.create_task(start_consumer_async(processor))
        tasks.append(task)

    try:
        # Wait for all consumers to complete
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Cancelling all async consumer tasks...")
        for task in tasks:
            task.cancel()

        # Wait for all tasks to complete cancellation
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("All async consumers stopped")


def shutdown_consumers() -> None:
    """Shutdown the shared thread pool for graceful application exit.

    Call this during application shutdown to ensure all background threads
    are properly closed. This is important for production deployments.
    """
    logger.info("Shutting down consumer async thread pool...")
    _kafka_thread_pool.shutdown(wait=True)
    logger.info("Consumer async thread pool shut down successfully")
