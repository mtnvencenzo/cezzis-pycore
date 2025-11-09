from abc import ABC, abstractmethod
from typing import Optional

from confluent_kafka import Consumer, Message

from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings


class IAsyncKafkaMessageProcessor(ABC):
    """Async version of IKafkaMessageProcessor interface.

    This extends the synchronous interface with async versions of the methods.
    You can implement this interface to create async message processors that work
    seamlessly with KafkaProducer delivery callbacks.
    """

    @staticmethod
    @abstractmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "IAsyncKafkaMessageProcessor":
        """Factory method to create a new instance of IAsyncKafkaMessageProcessor.

        Args:
            kafka_settings (KafkaConsumerSettings): The Kafka consumer settings.

        Returns:
            IAsyncKafkaMessageProcessor: A new instance of IAsyncKafkaMessageProcessor.
        """
        pass

    @abstractmethod
    def kafka_settings(self) -> KafkaConsumerSettings:
        """Get the Kafka consumer settings.

        Returns:
            KafkaConsumerSettings: The Kafka consumer settings.
        """
        pass

    @abstractmethod
    async def consumer_creating(self) -> None:
        """Handle actions to be taken when a consumer is being created.

        Args:   None
        """
        pass

    @abstractmethod
    async def consumer_created(self, consumer: Optional[Consumer]) -> None:
        """Handle actions to be taken when a consumer has been created.

        Args:
            consumer (Consumer | None): The Kafka consumer that has been created.
        """
        pass

    @abstractmethod
    async def message_received(self, msg: Message) -> None:
        """Process a Kafka message asynchronously.

        Args:
            msg (Message): The Kafka message to process.
        """
        pass

    @abstractmethod
    async def message_error_received(self, msg: Message) -> None:
        """Handle errors encountered during message processing asynchronously.

        Args:
            msg (Message): The Kafka message that caused the error.
        """
        pass

    @abstractmethod
    async def consumer_subscribed(self) -> None:
        """Handle actions to be taken when a consumer is subscribed.

        Args:   None
        """
        pass

    @abstractmethod
    async def consumer_stopping(self) -> None:
        """Handle actions to be taken when a consumer is stopping.

        Args:   None
        """
        pass

    @abstractmethod
    async def message_partition_reached(self, msg: Message) -> None:
        """Handle actions to be taken when a message partition is reached.

        Args:
            msg (Message): The Kafka message indicating the partition.
        """
        pass
