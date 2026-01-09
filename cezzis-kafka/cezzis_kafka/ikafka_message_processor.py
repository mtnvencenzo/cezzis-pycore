from abc import ABC, abstractmethod

from confluent_kafka import Consumer, Message

from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings


class IKafkaMessageProcessor(ABC):
    @staticmethod
    @abstractmethod
    def CreateNew(
        kafka_settings: KafkaConsumerSettings,
    ) -> "IKafkaMessageProcessor":
        """Factory method to create a new instance of IKafkaMessageProcessor.

        Args:
            kafka_settings (KafkaConsumerSettings): The Kafka consumer settings.

        Returns:
            IKafkaMessageProcessor: A new instance of IKafkaMessageProcessor.
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
    def consumer_creating(self) -> None:
        """Handle actions to be taken when a consumer is being created.

        Args:   None
        """
        pass

    @abstractmethod
    def consumer_created(self, consumer: Consumer | None) -> None:
        """Handle actions to be taken when a consumer has been created.

        Args:
            consumer (Consumer | None): The Kafka consumer that has been created.
        """
        pass

    @abstractmethod
    def message_received(self, msg: Message) -> None:
        """Process a Kafka message.

        Args:
            msg (Message): The Kafka message to process.
        """
        pass

    @abstractmethod
    def message_error_received(self, msg: Message) -> None:
        """Handle errors encountered during message processing.

        Args:
            msg (Message): The Kafka message that caused the error.
        """
        pass

    @abstractmethod
    def consumer_subscribed(self) -> None:
        """Handle actions to be taken when a consumer is subscribed.

        Args:   None
        """
        pass

    @abstractmethod
    def consumer_stopping(self) -> None:
        """Handle actions to be taken when a consumer is stopping.

        Args:   None
        """
        pass

    @abstractmethod
    def message_partition_reached(self, msg: Message) -> None:
        """Handle actions to be taken when a message partition is reached.

        Args:
            msg (Message): The Kafka message indicating the partition.
        """
        pass
