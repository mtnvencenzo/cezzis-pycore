import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Type, TypeVar

from confluent_kafka import Consumer, KafkaError, Message

from cezzis_kafka.iasync_kafka_message_processor import IAsyncKafkaMessageProcessor
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings

logger = logging.getLogger(__name__)

TAsyncProcessor = TypeVar("TAsyncProcessor", bound=IAsyncKafkaMessageProcessor)
"""Type variable for IAsyncKafkaMessageProcessor or its subclasses."""

# Configurable thread pool size for production flexibility
_DEFAULT_THREAD_POOL_SIZE = int(os.getenv("KAFKA_ASYNC_THREAD_POOL_SIZE", "16"))

# Shared thread pool for all Kafka async operations - production optimization
# Sized to handle multiple consumer groups and topics efficiently
_kafka_thread_pool = ThreadPoolExecutor(max_workers=_DEFAULT_THREAD_POOL_SIZE, thread_name_prefix="kafka_async")


async def start_consumer_async(processor: IAsyncKafkaMessageProcessor) -> None:
    """Start async Kafka consumer.

    Args:
        processor: Async message processor implementation
    """
    logger.info("Starting async Kafka consumer...")

    # Create consumer in thread pool since kafka client is synchronous
    consumer = await _create_consumer_async(processor)
    if consumer is None:
        return

    try:
        await _subscribe_consumer_async(consumer, processor)
        await _start_polling_async(consumer, processor)

    except asyncio.CancelledError:
        logger.info("Async consumer cancelled, shutting down...")
        raise
    except Exception:
        logger.error("Unexpected error in async consumer", exc_info=True)
        raise
    finally:
        await _close_consumer_async(consumer, processor)


async def _create_consumer_async(processor: IAsyncKafkaMessageProcessor) -> Optional[Consumer]:
    """Create Kafka consumer asynchronously."""

    await processor.consumer_creating()

    def create_consumer():
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": processor.kafka_settings().bootstrap_servers,
                    "group.id": processor.kafka_settings().consumer_group,
                    "auto.offset.reset": processor.kafka_settings().auto_offset_reset,
                    "max.poll.interval.ms": processor.kafka_settings().max_poll_interval_ms,
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
            msg = await loop.run_in_executor(_kafka_thread_pool, poll_message, 1.0)

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
