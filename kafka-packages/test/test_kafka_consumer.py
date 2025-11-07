from multiprocessing.synchronize import Event as EventClass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import Consumer, KafkaError, Message

# Import private functions for testing
import cezzis_kafka.kafka_consumer as kafka_consumer_module
from cezzis_kafka.ikafka_message_processor import IKafkaMessageProcessor
from cezzis_kafka.kafka_consumer import (
    spawn_consumers,
    start_consumer,
)
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings


class MockProcessor(IKafkaMessageProcessor):
    """Mock implementation of IKafkaMessageProcessor for testing."""

    def __init__(self, settings: KafkaConsumerSettings):
        self._settings = settings
        self.consumer_creating_called = False
        self.consumer_created_called = False
        self.message_received_called = False
        self.message_error_received_called = False
        self.consumer_subscribed_called = False
        self.consumer_stopping_called = False
        self.message_partition_reached_called = False

    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "MockProcessor":
        return MockProcessor(kafka_settings)

    def kafka_settings(self) -> KafkaConsumerSettings:
        return self._settings

    def consumer_creating(self) -> None:
        self.consumer_creating_called = True

    def consumer_created(self, consumer: Consumer | None) -> None:
        self.consumer_created_called = True

    def message_received(self, msg: Message) -> None:
        self.message_received_called = True

    def message_error_received(self, msg: Message) -> None:
        self.message_error_received_called = True

    def consumer_subscribed(self) -> None:
        self.consumer_subscribed_called = True

    def consumer_stopping(self) -> None:
        self.consumer_stopping_called = True

    def message_partition_reached(self, msg: Message) -> None:
        self.message_partition_reached_called = True


@pytest.fixture
def mock_settings() -> KafkaConsumerSettings:
    """Fixture providing test Kafka consumer settings."""
    return KafkaConsumerSettings(
        consumer_id=1,
        bootstrap_servers="localhost:9092",
        consumer_group="test-group",
        topic_name="test-topic",
        num_consumers=1,
    )


@pytest.fixture
def mock_processor(mock_settings: KafkaConsumerSettings) -> MockProcessor:
    """Fixture providing a mock processor."""
    return MockProcessor(mock_settings)


@pytest.fixture
def mock_consumer() -> MagicMock:
    """Fixture providing a mock Kafka consumer."""
    consumer = MagicMock(spec=Consumer)
    consumer.poll = MagicMock(return_value=None)
    consumer.commit = MagicMock()
    consumer.close = MagicMock()
    consumer.subscribe = MagicMock()
    return consumer


@pytest.fixture
def stop_event() -> EventClass:
    """Fixture providing a stop event."""
    from multiprocessing import Event

    return Event()


class TestCreateConsumer:
    """Tests for _create_consumer function."""

    @patch("cezzis_kafka.kafka_consumer.Consumer")
    def test_creates_consumer_successfully(self, mock_consumer_class: MagicMock, mock_processor: MockProcessor) -> None:
        """Test that consumer is created with correct configuration."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance

        result = kafka_consumer_module._create_consumer(mock_processor)

        # Verify Consumer was instantiated with correct config
        mock_consumer_class.assert_called_once_with(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": "test-group",
                "auto.offset.reset": "earliest",
            }
        )

        # Verify lifecycle hooks were called
        assert mock_processor.consumer_creating_called
        assert mock_processor.consumer_created_called
        assert result == mock_consumer_instance

    @patch("cezzis_kafka.kafka_consumer.Consumer")
    def test_returns_none_on_exception(self, mock_consumer_class: MagicMock, mock_processor: MockProcessor) -> None:
        """Test that None is returned when consumer creation fails."""
        mock_consumer_class.side_effect = Exception("Connection failed")

        result = kafka_consumer_module._create_consumer(mock_processor)

        assert result is None
        assert mock_processor.consumer_creating_called


class TestSubscribeConsumer:
    """Tests for _subscribe_consumer function."""

    def test_subscribes_to_topic_successfully(self, mock_consumer: MagicMock, mock_processor: MockProcessor) -> None:
        """Test that consumer subscribes to the correct topic."""
        kafka_consumer_module._subscribe_consumer(mock_consumer, mock_processor)

        mock_consumer.subscribe.assert_called_once_with(["test-topic"])
        assert mock_processor.consumer_subscribed_called

    def test_raises_exception_on_subscription_error(
        self, mock_consumer: MagicMock, mock_processor: MockProcessor
    ) -> None:
        """Test that exceptions are raised when subscription fails."""
        mock_consumer.subscribe.side_effect = Exception("Subscription failed")

        with pytest.raises(Exception, match="Subscription failed"):
            kafka_consumer_module._subscribe_consumer(mock_consumer, mock_processor)


class TestStartPolling:
    """Tests for _start_polling function."""

    def test_polls_and_processes_message(
        self, mock_consumer: MagicMock, mock_processor: MockProcessor, stop_event: EventClass
    ) -> None:
        """Test that messages are polled and processed correctly."""
        # Create a mock message
        mock_message = MagicMock(spec=Message)
        mock_message.error.return_value = None
        mock_message.value.return_value = b"test message"

        # Setup poll to return message once, then None (which will continue loop until stop_event)
        poll_count = [0]

        def poll_side_effect(timeout: float) -> MagicMock | None:
            poll_count[0] += 1
            if poll_count[0] == 1:
                return mock_message
            else:
                stop_event.set()  # Stop after first message
                return None

        mock_consumer.poll.side_effect = poll_side_effect

        kafka_consumer_module._start_polling(stop_event, mock_consumer, mock_processor)

        assert mock_processor.message_received_called
        assert mock_consumer.poll.call_count >= 1

    def test_handles_partition_eof_error(
        self, mock_consumer: MagicMock, mock_processor: MockProcessor, stop_event: EventClass
    ) -> None:
        """Test that partition EOF errors are handled correctly."""
        # Create mock message with partition EOF error
        mock_message = MagicMock(spec=Message)
        mock_error = MagicMock()
        mock_error.code.return_value = KafkaError._PARTITION_EOF
        mock_message.error.return_value = mock_error
        mock_message.partition.return_value = 0

        poll_count = [0]

        def poll_side_effect(timeout: float) -> MagicMock | None:
            poll_count[0] += 1
            if poll_count[0] == 1:
                return mock_message
            else:
                stop_event.set()
                return None

        mock_consumer.poll.side_effect = poll_side_effect

        kafka_consumer_module._start_polling(stop_event, mock_consumer, mock_processor)

        assert mock_processor.message_partition_reached_called

    def test_handles_kafka_error(
        self, mock_consumer: MagicMock, mock_processor: MockProcessor, stop_event: EventClass
    ) -> None:
        """Test that Kafka errors are handled correctly."""
        # Create mock message with error
        mock_message = MagicMock(spec=Message)
        mock_error = MagicMock()
        mock_error.code.return_value = KafkaError.UNKNOWN
        mock_message.error.return_value = mock_error
        mock_message.partition.return_value = 0

        poll_count = [0]

        def poll_side_effect(timeout: float) -> MagicMock | None:
            poll_count[0] += 1
            if poll_count[0] == 1:
                return mock_message
            else:
                stop_event.set()
                return None

        mock_consumer.poll.side_effect = poll_side_effect

        kafka_consumer_module._start_polling(stop_event, mock_consumer, mock_processor)

        assert mock_processor.message_error_received_called

    def test_stops_when_event_is_set(
        self, mock_consumer: MagicMock, mock_processor: MockProcessor, stop_event: EventClass
    ) -> None:
        """Test that polling stops when stop_event is set."""
        stop_event.set()  # Set event before starting

        kafka_consumer_module._start_polling(stop_event, mock_consumer, mock_processor)

        # Poll should not be called since event is already set
        mock_consumer.poll.assert_not_called()


class TestCloseConsumer:
    """Tests for _close_consumer function."""

    def test_closes_consumer_successfully(self, mock_consumer: MagicMock, mock_processor: MockProcessor) -> None:
        """Test that consumer is closed properly."""
        kafka_consumer_module._close_consumer(mock_consumer, mock_processor)

        mock_consumer.commit.assert_called_once()
        mock_consumer.close.assert_called_once()
        assert mock_processor.consumer_stopping_called

    def test_closes_consumer_even_if_commit_fails(
        self, mock_consumer: MagicMock, mock_processor: MockProcessor
    ) -> None:
        """Test that consumer is closed even if commit fails."""
        mock_consumer.commit.side_effect = Exception("Commit failed")

        kafka_consumer_module._close_consumer(mock_consumer, mock_processor)

        # Consumer should still be closed
        mock_consumer.close.assert_called_once()
        assert mock_processor.consumer_stopping_called


class TestStartConsumer:
    """Tests for start_consumer function."""

    @patch("cezzis_kafka.kafka_consumer._close_consumer")
    @patch("cezzis_kafka.kafka_consumer._start_polling")
    @patch("cezzis_kafka.kafka_consumer._subscribe_consumer")
    @patch("cezzis_kafka.kafka_consumer._create_consumer")
    def test_starts_consumer_successfully(
        self,
        mock_create: MagicMock,
        mock_subscribe: MagicMock,
        mock_poll: MagicMock,
        mock_close: MagicMock,
        mock_processor: MockProcessor,
        stop_event: EventClass,
    ) -> None:
        """Test that consumer starts and executes full lifecycle."""
        mock_consumer = MagicMock()
        mock_create.return_value = mock_consumer

        start_consumer(stop_event, mock_processor)

        mock_create.assert_called_once_with(mock_processor)
        mock_subscribe.assert_called_once_with(mock_consumer, mock_processor)
        mock_poll.assert_called_once_with(stop_event, mock_consumer, mock_processor)
        mock_close.assert_called_once_with(mock_consumer, mock_processor)

    @patch("cezzis_kafka.kafka_consumer._create_consumer")
    def test_returns_early_if_consumer_creation_fails(
        self, mock_create: MagicMock, mock_processor: MockProcessor, stop_event: EventClass
    ) -> None:
        """Test that function returns early if consumer creation fails."""
        mock_create.return_value = None

        start_consumer(stop_event, mock_processor)

        # Should return early, no other functions called
        mock_create.assert_called_once()

    @patch("cezzis_kafka.kafka_consumer._close_consumer")
    @patch("cezzis_kafka.kafka_consumer._start_polling")
    @patch("cezzis_kafka.kafka_consumer._subscribe_consumer")
    @patch("cezzis_kafka.kafka_consumer._create_consumer")
    def test_handles_keyboard_interrupt(
        self,
        mock_create: MagicMock,
        mock_subscribe: MagicMock,
        mock_poll: MagicMock,
        mock_close: MagicMock,
        mock_processor: MockProcessor,
        stop_event: EventClass,
    ) -> None:
        """Test that KeyboardInterrupt is handled gracefully."""
        mock_consumer = MagicMock()
        mock_create.return_value = mock_consumer
        mock_poll.side_effect = KeyboardInterrupt("User interrupted")

        start_consumer(stop_event, mock_processor)

        # Consumer should still be closed
        mock_close.assert_called_once_with(mock_consumer, mock_processor)

    @patch("cezzis_kafka.kafka_consumer._close_consumer")
    @patch("cezzis_kafka.kafka_consumer._start_polling")
    @patch("cezzis_kafka.kafka_consumer._subscribe_consumer")
    @patch("cezzis_kafka.kafka_consumer._create_consumer")
    def test_handles_unexpected_exception(
        self,
        mock_create: MagicMock,
        mock_subscribe: MagicMock,
        mock_poll: MagicMock,
        mock_close: MagicMock,
        mock_processor: MockProcessor,
        stop_event: EventClass,
    ) -> None:
        """Test that unexpected exceptions are handled and consumer is closed."""
        mock_consumer = MagicMock()
        mock_create.return_value = mock_consumer
        mock_poll.side_effect = Exception("Unexpected error")

        start_consumer(stop_event, mock_processor)

        # Consumer should still be closed
        mock_close.assert_called_once_with(mock_consumer, mock_processor)


class TestSpawnConsumers:
    """Tests for spawn_consumers function."""

    @patch("cezzis_kafka.kafka_consumer.Process")
    def test_spawns_correct_number_of_consumers(self, mock_process_class: MagicMock, stop_event: EventClass) -> None:
        """Test that correct number of consumer processes are spawned."""
        mock_process_instances: list[MagicMock] = []
        for i in range(3):
            mock_proc = MagicMock()
            mock_proc.pid = 1000 + i
            mock_proc.exitcode = 0
            mock_proc.name = f"Process-{i}"
            mock_process_instances.append(mock_proc)

        mock_process_class.side_effect = mock_process_instances

        spawn_consumers(
            factory_type=MockProcessor,
            num_consumers=3,
            stop_event=stop_event,
            bootstrap_servers="localhost:9092",
            consumer_group="test-group",
            topic_name="test-topic",
        )

        # Verify 3 processes were created
        assert mock_process_class.call_count == 3

        # Verify each process was started and joined
        for mock_proc in mock_process_instances:
            mock_proc.start.assert_called_once()
            mock_proc.join.assert_called_once()

    @patch("cezzis_kafka.kafka_consumer.Process")
    def test_creates_processors_with_unique_ids(self, mock_process_class: MagicMock, stop_event: EventClass) -> None:
        """Test that each spawned consumer gets a unique consumer_id."""
        captured_settings: list[KafkaConsumerSettings] = []

        def capture_process(*args: Any, **kwargs: Any) -> MagicMock:
            # Extract the target function and its arguments
            args_tuple = kwargs.get("args", ())
            if len(args_tuple) > 1:
                processor = args_tuple[1]
                captured_settings.append(processor.kafka_settings())

            mock_proc = MagicMock()
            mock_proc.pid = 1000
            mock_proc.exitcode = 0
            mock_proc.name = "Process-0"
            return mock_proc

        mock_process_class.side_effect = capture_process

        spawn_consumers(
            factory_type=MockProcessor,
            num_consumers=3,
            stop_event=stop_event,
            bootstrap_servers="localhost:9092",
            consumer_group="test-group",
            topic_name="test-topic",
        )

        # Verify consumer_ids are 0, 1, 2
        consumer_ids = [settings.consumer_id for settings in captured_settings]
        assert consumer_ids == [0, 1, 2]

        # Verify all have the same group and topic
        for settings in captured_settings:
            assert settings.bootstrap_servers == "localhost:9092"
            assert settings.consumer_group == "test-group"
            assert settings.topic_name == "test-topic"
            assert settings.num_consumers == 3
