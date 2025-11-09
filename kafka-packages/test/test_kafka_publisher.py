"""
Comprehensive tests for KafkaPublisher class using KafkaPublisherSettings.
"""

import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka import KafkaError, Producer

from cezzis_kafka.kafka_publisher import KafkaPublisher
from cezzis_kafka.kafka_publisher_settings import KafkaPublisherSettings


class TestKafkaPublisher:
    """Test suite for KafkaPublisher class."""

    @pytest.fixture
    def basic_settings(self):
        """Create basic KafkaPublisherSettings for testing."""
        return KafkaPublisherSettings(bootstrap_servers="localhost:9092")

    @pytest.fixture
    def custom_settings(self):
        """Create custom KafkaPublisherSettings for testing."""
        on_delivery_callback = Mock()
        return KafkaPublisherSettings(
            bootstrap_servers="kafka1:9092,kafka2:9092",
            max_retries=5,
            retry_backoff_ms=200,
            retry_backoff_max_ms=2000,
            delivery_timeout_ms=600000,
            request_timeout_ms=60000,
            on_delivery=on_delivery_callback,
            producer_config={"batch.size": 32768, "linger.ms": 20},
        )

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_init_with_basic_settings(self, mock_producer, basic_settings):
        """Test KafkaPublisher initialization with basic settings."""
        publisher = KafkaPublisher(basic_settings)

        # Verify Producer was called with correct configuration
        expected_config = {
            "bootstrap.servers": "localhost:9092",
            "acks": "all",
            "retries": 3,  # Default max_retries
            "retry.backoff.ms": 100,
            "retry.backoff.max.ms": 1000,
            "delivery.timeout.ms": 300000,
            "request.timeout.ms": 30000,
            "max.in.flight.requests.per.connection": 1,
            "enable.idempotence": True,
            "compression.type": "snappy",
        }
        mock_producer.assert_called_once_with(expected_config)

        # Verify publisher attributes
        assert publisher.settings == basic_settings
        assert publisher.broker_url == "localhost:9092"

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_init_with_custom_settings(self, mock_producer, custom_settings):
        """Test KafkaPublisher initialization with custom settings."""
        publisher = KafkaPublisher(custom_settings)

        # Verify Producer was called with merged configuration
        expected_config = {
            "bootstrap.servers": "kafka1:9092,kafka2:9092",
            "acks": "all",
            "retries": 5,  # Custom max_retries
            "retry.backoff.ms": 200,
            "retry.backoff.max.ms": 2000,
            "delivery.timeout.ms": 600000,
            "request.timeout.ms": 60000,
            "max.in.flight.requests.per.connection": 1,
            "enable.idempotence": True,
            "compression.type": "snappy",
            "batch.size": 32768,  # From producer_config
            "linger.ms": 20,  # From producer_config
        }
        mock_producer.assert_called_once_with(expected_config)

        assert publisher.settings == custom_settings
        assert publisher.broker_url == "kafka1:9092,kafka2:9092"

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_send_with_auto_generated_message_id(self, mock_producer, basic_settings):
        """Test sending a message with auto-generated message ID."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance

        publisher = KafkaPublisher(basic_settings)

        # Mock the message ID generation
        with patch.object(publisher, "_generate_message_id", return_value="test-msg-123"):
            message_id = publisher.send(
                topic="test-topic", message="Hello, World!", key="test-key", headers={"custom": "header"}
            )

        # Verify the message ID was generated
        assert message_id == "test-msg-123"

        # Verify producer.produce was called with correct parameters
        mock_producer_instance.produce.assert_called_once_with(
            topic="test-topic",
            value="Hello, World!",
            key="test-key",
            headers={"custom": "header", "message_id": "test-msg-123"},
            on_delivery=basic_settings.on_delivery,
        )

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_send_with_provided_message_id(self, mock_producer, custom_settings):
        """Test sending a message with provided message ID."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance

        publisher = KafkaPublisher(custom_settings)

        message_id = publisher.send(
            topic="test-topic",
            message=b"Binary message",
            message_id="custom-msg-id",
            metadata={"correlation_id": "12345"},
        )

        assert message_id == "custom-msg-id"

        # Verify producer.produce was called
        mock_producer_instance.produce.assert_called_once_with(
            topic="test-topic",
            value=b"Binary message",
            key=None,
            headers={"message_id": "custom-msg-id"},
            on_delivery=custom_settings.on_delivery,
        )

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_send_with_producer_exception(self, mock_producer, basic_settings):
        """Test handling of producer exceptions during send."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance

        # Make producer.produce raise an exception
        test_exception = Exception("Producer error")
        mock_producer_instance.produce.side_effect = test_exception

        publisher = KafkaPublisher(basic_settings)

        # Verify the exception is re-raised
        with pytest.raises(Exception, match="Producer error"):
            publisher.send(topic="test-topic", message="test message")

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_send_various_message_types(self, mock_producer, basic_settings):
        """Test sending different message types."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance

        publisher = KafkaPublisher(basic_settings)

        test_cases = [
            ("string message", "string message"),
            (b"bytes message", b"bytes message"),
            ("123", "123"),
        ]

        for i, (message, expected) in enumerate(test_cases):
            mock_producer_instance.produce.reset_mock()

            with patch.object(publisher, "_generate_message_id", return_value=f"msg-{i}"):
                publisher.send(topic="test-topic", message=message)

            call_args = mock_producer_instance.produce.call_args
            assert call_args[1]["value"] == expected

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_flush_success(self, mock_producer, basic_settings):
        """Test successful flush operation."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.flush.return_value = 0  # No remaining messages

        publisher = KafkaPublisher(basic_settings)
        publisher.flush(timeout=5.0)

        mock_producer_instance.flush.assert_called_once_with(5.0)

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_flush_with_remaining_messages(self, mock_producer, basic_settings):
        """Test flush with some messages remaining."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.flush.return_value = 3  # 3 remaining messages

        publisher = KafkaPublisher(basic_settings)

        with patch("cezzis_kafka.kafka_publisher.logger") as mock_logger:
            publisher.flush(timeout=2.0)

        mock_producer_instance.flush.assert_called_once_with(2.0)
        mock_logger.warning.assert_called_once_with("Failed to deliver 3 messages within timeout")

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_close(self, mock_producer, basic_settings):
        """Test close operation."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance
        mock_producer_instance.flush.return_value = 0

        publisher = KafkaPublisher(basic_settings)

        with patch("cezzis_kafka.kafka_publisher.logger") as mock_logger:
            publisher.close()

        # Should flush first, then log success
        mock_producer_instance.flush.assert_called_once_with(10.0)  # Default timeout
        mock_logger.info.assert_called_with("Kafka publisher closed successfully")

    def test_generate_message_id(self, basic_settings):
        """Test message ID generation."""
        publisher = KafkaPublisher(basic_settings)

        # Test message ID format
        message_id = publisher._generate_message_id("test-topic")

        # Should be in format: topic_timestamp_randomhex
        parts = message_id.split("_")
        assert len(parts) == 3
        assert parts[0] == "test-topic"
        assert parts[1].isdigit()  # timestamp
        assert len(parts[2]) == 8  # 8-char hex suffix
        assert all(c in "0123456789abcdef" for c in parts[2])

    def test_generate_message_id_uniqueness(self, basic_settings):
        """Test that generated message IDs are unique."""
        publisher = KafkaPublisher(basic_settings)

        # Generate multiple IDs
        ids = [publisher._generate_message_id("test-topic") for _ in range(100)]

        # All should be unique
        assert len(set(ids)) == 100

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_broker_url_property(self, mock_producer, basic_settings):
        """Test broker_url property."""
        publisher = KafkaPublisher(basic_settings)
        assert publisher.broker_url == basic_settings.bootstrap_servers

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_settings_stored(self, mock_producer, basic_settings):
        """Test that settings are properly stored."""
        publisher = KafkaPublisher(basic_settings)
        assert publisher.settings is basic_settings

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_send_headers_handling(self, mock_producer, basic_settings):
        """Test proper headers handling in send method."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance

        publisher = KafkaPublisher(basic_settings)

        # Test with no headers
        with patch.object(publisher, "_generate_message_id", return_value="msg-1"):
            publisher.send(topic="test-topic", message="test")

        call_args = mock_producer_instance.produce.call_args
        assert call_args[1]["headers"] == {"message_id": "msg-1"}

        # Test with existing headers
        mock_producer_instance.produce.reset_mock()
        with patch.object(publisher, "_generate_message_id", return_value="msg-2"):
            publisher.send(
                topic="test-topic", message="test", headers={"existing": "header", "message_id": "should-be-overridden"}
            )

        call_args = mock_producer_instance.produce.call_args
        expected_headers = {"existing": "header", "message_id": "msg-2"}
        assert call_args[1]["headers"] == expected_headers

    @patch("cezzis_kafka.kafka_publisher.Producer")
    def test_concurrent_sends(self, mock_producer, basic_settings):
        """Test multiple concurrent send operations."""
        mock_producer_instance = Mock()
        mock_producer.return_value = mock_producer_instance

        publisher = KafkaPublisher(basic_settings)

        # Send multiple messages
        message_ids = []
        for i in range(10):
            with patch.object(publisher, "_generate_message_id", return_value=f"msg-{i}"):
                msg_id = publisher.send(topic=f"topic-{i}", message=f"message-{i}", key=f"key-{i}")
                message_ids.append(msg_id)

        # Verify all were sent
        assert len(message_ids) == 10
        assert mock_producer_instance.produce.call_count == 10

        # Verify each call had correct parameters
        calls = mock_producer_instance.produce.call_args_list
        for i, call in enumerate(calls):
            assert call[1]["topic"] == f"topic-{i}"
            assert call[1]["value"] == f"message-{i}"
            assert call[1]["key"] == f"key-{i}"
            assert call[1]["headers"]["message_id"] == f"msg-{i}"


class TestKafkaPublisherErrorHandling:
    """Test error handling scenarios for KafkaPublisher."""

    def test_producer_creation_failure(self):
        """Test handling of Producer creation failure."""
        settings = KafkaPublisherSettings(bootstrap_servers="invalid:9092")

        with patch("cezzis_kafka.kafka_publisher.Producer", side_effect=Exception("Connection failed")):
            with pytest.raises(Exception, match="Connection failed"):
                KafkaPublisher(settings)
