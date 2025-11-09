"""Simple async consumer test to demonstrate the async functionality."""

import asyncio
import logging
from typing import Optional

from confluent_kafka import Consumer, Message

from cezzis_kafka.async_kafka_consumer import shutdown_async_kafka, start_consumer_async
from cezzis_kafka.iasync_kafka_message_processor import IAsyncKafkaMessageProcessor
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

logger = logging.getLogger(__name__)


class SimpleAsyncProcessor(IAsyncKafkaMessageProcessor):
    """Simple async processor that just logs messages."""

    def __init__(self):
        pass

    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "SimpleAsyncProcessor":
        """Factory method to create a new instance."""
        return SimpleAsyncProcessor()

    def kafka_settings(self) -> KafkaConsumerSettings:
        """Returns the Kafka consumer settings."""
        return KafkaConsumerSettings(
            consumer_id=1,
            bootstrap_servers="localhost:9092",
            consumer_group="async_test_group",
            topic_name="test_topic",
        )

    async def consumer_creating(self) -> None:
        """Hook called when consumer is being created."""
        logger.info("🔧 Consumer creating...")

    async def consumer_created(self, consumer: Optional[Consumer]) -> None:
        """Hook called when consumer has been created."""
        logger.info("✅ Consumer created successfully")

    async def consumer_subscribed(self) -> None:
        """Hook called when consumer subscribes to topic."""
        logger.info("📡 Consumer subscribed to topic")

    async def consumer_stopping(self) -> None:
        """Hook called when consumer is stopping."""
        logger.info("⏹️ Consumer stopping...")

    async def message_error_received(self, msg: Message) -> None:
        """Handle errors in received messages."""
        logger.error(f"❌ Error in message: {msg.error()}")

    async def message_partition_reached(self, msg: Message) -> None:
        """Handle reaching end of partition."""
        logger.info("📍 Reached end of partition")

    async def message_received(self, msg: Message) -> None:
        """Process received message asynchronously."""
        try:
            # Decode message safely
            msg_key = msg.key()
            key = msg_key.decode("utf-8") if msg_key is not None else "no-key"

            msg_value = msg.value()
            value = msg_value.decode("utf-8") if msg_value is not None else ""

            logger.info(f"✅ Async received - Key: {key}, Value: {value}")

            # Simulate some async processing
            await asyncio.sleep(0.1)

            logger.info(f"✅ Async processed - Key: {key}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)


async def main():
    """Main async function."""
    logger.info("🚀 Starting simple async consumer test...")

    processor = SimpleAsyncProcessor()

    try:
        await start_consumer_async(processor)
    except KeyboardInterrupt:
        logger.info("⏹️ Received keyboard interrupt")
    except Exception as e:
        logger.error(f"❌ Error in async consumer: {e}", exc_info=True)


if __name__ == "__main__":
    print("🔄 This async consumer works with KafkaProducer delivery callbacks!")
    print("💡 Use this instead of spawn_consumers() for better compatibility.")
    print("⚡ Press Ctrl+C to stop...")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Async consumer stopped gracefully")
    finally:
        # Important: Shutdown thread pool for production deployments
        shutdown_async_kafka()
        print("🧹 Thread pool cleaned up")
