"""
Comprehensive tests for KafkaPublisher class using KafkaPublisherSettings.
"""

import time
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, Producer

from cezzis_kafka.delivery_handler import DeliveryHandler
from cezzis_kafka.kafka_publisher import KafkaPublisher
from cezzis_kafka.kafka_publisher_settings import KafkaPublisherSettings


class TestKafkaPublisher:
    """Test suite for KafkaPublisher class."""

    @pytest.fixture
    def mock_producer(self, mocker):
        """Create a mock Kafka Producer."""
        return mocker.patch("cezzis_kafka.kafka_publisher.Producer")

    @pytest.fixture
    def mock_delivery_handler(self, mocker):
        """Create a mock DeliveryHandler."""
        return mocker.patch("cezzis_kafka.kafka_publisher.DeliveryHandler")

    @pytest.fixture
    def basic_settings(self):
        """Basic settings for testing."""
        return KafkaPublisherSettings(bootstrap_servers="localhost:9092")

    @pytest.fixture
    def advanced_settings(self):
        """Advanced settings for testing."""
        return KafkaPublisherSettings(
            bootstrap_servers="kafka1:9092,kafka2:9092",
            max_retries=2,
            producer_config={"batch.size": 16384, "linger.ms": 10},
        )

    def test_init_default_config(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test publisher initialization with default configuration."""
        _ = KafkaPublisher(basic_settings)

        # Verify Producer was created with correct default config
        expected_config = {
            "bootstrap.servers": "localhost:9092",
            "acks": "all",
            "retries": 0,
            "max.in.flight.requests.per.connection": 1,
            "enable.idempotence": True,
            "compression.type": "snappy",
        }
        mock_producer.assert_called_once_with(expected_config)

        # Verify DeliveryHandler was created with correct parameters
        mock_delivery_handler.assert_called_once_with(
            max_retries=3,
            metrics_callback=None,
            bootstrap_servers="localhost:9092",
            retry_producer=mock_producer.return_value,
        )

    def test_init_custom_config(self, mock_producer, mock_delivery_handler):
        """Test publisher initialization with custom configuration."""
        metrics_callback = MagicMock()

        custom_settings = KafkaPublisherSettings(
            bootstrap_servers="kafka1:9092,kafka2:9092",
            max_retries=2,
            metrics_callback=metrics_callback,
            producer_config={"batch.size": 16384, "linger.ms": 10},
        )

        _ = KafkaPublisher(custom_settings)

        # Verify custom producer config was merged
        expected_config = {
            "bootstrap.servers": "kafka1:9092,kafka2:9092",
            "acks": "all",
            "retries": 0,
            "max.in.flight.requests.per.connection": 1,
            "enable.idempotence": True,
            "compression.type": "snappy",
            "batch.size": 16384,
            "linger.ms": 10,
        }
        mock_producer.assert_called_once_with(expected_config)

        # Verify DeliveryHandler parameters
        mock_delivery_handler.assert_called_once_with(
            max_retries=2,
            metrics_callback=metrics_callback,
            bootstrap_servers="kafka1:9092,kafka2:9092",
            retry_producer=mock_producer.return_value,
        )

    def test_send_with_auto_generated_message_id(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test sending message with auto-generated message ID."""
        publisher = KafkaPublisher(basic_settings)
        mock_producer_instance = mock_producer.return_value
        mock_handler_instance = mock_delivery_handler.return_value

        # Mock message ID generation
        with patch.object(publisher, "_generate_message_id", return_value="test-msg-123") as mock_gen_id:
            message_id = publisher.send(
                topic="test-topic", message="Hello, World!", key="test-key", headers={"custom": "header"}
            )

        # Verify message ID was generated
        mock_gen_id.assert_called_once_with("test-topic")
        assert message_id == "test-msg-123"

        # Verify tracking was set up
        expected_original_data = {
            "value": "Hello, World!",
            "key": "test-key",
            "headers": {"custom": "header", "message_id": "test-msg-123"},
            "topic": "test-topic",
        }
        mock_handler_instance.track_message.assert_called_once_with(
            "test-msg-123", "test-topic", {}, expected_original_data
        )

        # Verify producer.produce was called correctly
        mock_producer_instance.produce.assert_called_once_with(
            topic="test-topic",
            value="Hello, World!",
            key="test-key",
            headers={"custom": "header", "message_id": "test-msg-123"},
            on_delivery=mock_handler_instance.handle_delivery,
        )

    def test_send_with_provided_message_id(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test sending message with provided message ID."""
        publisher = KafkaPublisher(basic_settings)
        mock_handler_instance = mock_delivery_handler.return_value

        message_id = publisher.send(
            topic="test-topic",
            message=b"Binary message",
            message_id="custom-msg-id",
            metadata={"correlation_id": "12345"},
        )

        assert message_id == "custom-msg-id"

        # Verify tracking includes metadata
        expected_original_data = {
            "value": b"Binary message",
            "key": None,
            "headers": {"message_id": "custom-msg-id"},
            "topic": "test-topic",
        }
        mock_handler_instance.track_message.assert_called_once_with(
            "custom-msg-id", "test-topic", {"correlation_id": "12345"}, expected_original_data
        )

    def test_send_producer_exception(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test handling of producer exceptions during send."""
        publisher = KafkaPublisher(basic_settings)
        mock_producer_instance = mock_producer.return_value
        mock_handler_instance = mock_delivery_handler.return_value

        # Make producer.produce raise an exception
        test_exception = Exception("Producer error")
        mock_producer_instance.produce.side_effect = test_exception

        with pytest.raises(Exception, match="Producer error"):
            publisher.send(topic="test-topic", message="test")

        # Verify message was removed from tracking on failure
        mock_handler_instance.remove_pending_message.assert_called_once()

    def test_generate_message_id(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test message ID generation."""
        publisher = KafkaPublisher(basic_settings)

        # Mock time to get predictable timestamp
        with patch("time.time", return_value=1234567890.123):
            with patch("uuid.uuid4") as mock_uuid:
                mock_uuid.return_value.hex = "abcdef1234567890"

                message_id = publisher._generate_message_id("test-topic")

        # Should include topic, timestamp, and UUID suffix
        assert message_id == "test-topic_1234567890123_abcdef12"

    def test_generate_message_id_uniqueness(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test that generated message IDs are unique."""
        publisher = KafkaPublisher(basic_settings)

        # Generate multiple message IDs
        ids = [publisher._generate_message_id("test-topic") for _ in range(100)]

        # All should be unique
        assert len(set(ids)) == 100

        # All should contain the topic name
        assert all("test-topic" in msg_id for msg_id in ids)

    def test_flush_success(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test successful flush operation."""
        publisher = KafkaPublisher(basic_settings)
        mock_producer_instance = mock_producer.return_value
        mock_producer_instance.flush.return_value = 0  # All messages delivered

        publisher.flush(timeout=5.0)

        mock_producer_instance.flush.assert_called_once_with(5.0)

    def test_flush_with_remaining_messages(self, mock_producer, mock_delivery_handler, basic_settings, caplog):
        """Test flush when some messages remain undelivered."""
        publisher = KafkaPublisher(basic_settings)
        mock_producer_instance = mock_producer.return_value
        mock_producer_instance.flush.return_value = 3  # 3 messages not delivered

        with caplog.at_level("WARNING"):
            publisher.flush()

        assert "Failed to deliver 3 messages within timeout" in caplog.text

    def test_close(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test publisher close operation."""
        publisher = KafkaPublisher(basic_settings)
        mock_producer_instance = mock_producer.return_value
        mock_handler_instance = mock_delivery_handler.return_value
        mock_producer_instance.flush.return_value = 0

        publisher.close()

        # Should flush first, then close delivery handler
        mock_producer_instance.flush.assert_called_once_with(10.0)
        mock_handler_instance.close.assert_called_once()

    def test_send_headers_handling(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test proper handling of different header types."""
        publisher = KafkaPublisher(basic_settings)
        mock_producer_instance = mock_producer.return_value

        # Test with mixed header types
        headers = {"string_header": "value", "bytes_header": b"bytes_value", "int_header": 42}

        publisher.send(topic="test-topic", message="test", headers=headers, message_id="test-msg")

        # Verify headers were preserved and message_id was added
        call_args = mock_producer_instance.produce.call_args
        final_headers = call_args[1]["headers"]

        assert final_headers["string_header"] == "value"
        assert final_headers["bytes_header"] == b"bytes_value"
        assert final_headers["int_header"] == 42
        assert final_headers["message_id"] == "test-msg"

    def test_send_no_headers(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test sending message without headers."""
        publisher = KafkaPublisher(basic_settings)
        mock_producer_instance = mock_producer.return_value

        publisher.send(topic="test-topic", message="test", message_id="test-msg")

        # Should create headers with just message_id
        call_args = mock_producer_instance.produce.call_args
        final_headers = call_args[1]["headers"]

        assert final_headers == {"message_id": "test-msg"}

    @pytest.mark.parametrize(
        "message_type,expected",
        [
            ("string message", "string message"),
            (b"bytes message", b"bytes message"),
            (123, 123),  # Should handle non-string types
        ],
    )
    def test_send_message_types(self, message_type, expected, mock_producer, mock_delivery_handler, basic_settings):
        """Test sending different message types."""
        publisher = KafkaPublisher(basic_settings)
        mock_producer_instance = mock_producer.return_value

        publisher.send(topic="test-topic", message=message_type, message_id="test-msg")

        call_args = mock_producer_instance.produce.call_args
        assert call_args[1]["value"] == expected

    def test_broker_url_property(self, mock_producer, mock_delivery_handler):
        """Test that broker_url property returns bootstrap_servers for backward compatibility."""
        settings = KafkaPublisherSettings(bootstrap_servers="kafka1:9092,kafka2:9092,kafka3:9092")
        publisher = KafkaPublisher(settings)

        assert publisher.broker_url == "kafka1:9092,kafka2:9092,kafka3:9092"

    def test_settings_stored(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test that settings are stored correctly."""
        publisher = KafkaPublisher(basic_settings)

        assert publisher.settings == basic_settings

    def test_integration_with_delivery_handler(self, mocker):
        """Test integration between publisher and delivery handler."""
        # Don't mock DeliveryHandler to test real integration
        mock_producer = mocker.patch("cezzis_kafka.kafka_publisher.Producer")
        mock_producer_instance = mock_producer.return_value

        settings = KafkaPublisherSettings(bootstrap_servers="localhost:9092", max_retries=2)

        publisher = KafkaPublisher(settings)

        # Send a message
        message_id = publisher.send(topic="test-topic", message="integration test", metadata={"test": "data"})

        # Verify producer was called with delivery handler callback
        call_args = mock_producer_instance.produce.call_args
        assert call_args[1]["on_delivery"] == publisher._delivery_handler.handle_delivery

        # Verify message is tracked
        assert message_id in publisher._delivery_handler._pending_messages

    def test_concurrent_sends(self, mock_producer, mock_delivery_handler, basic_settings):
        """Test handling of concurrent send operations."""
        import queue
        import threading

        publisher = KafkaPublisher(basic_settings)
        results = queue.Queue()
        errors = queue.Queue()

        def send_message(i):
            try:
                msg_id = publisher.send(topic="test-topic", message=f"Message {i}", message_id=f"msg-{i}")
                results.put(msg_id)
            except Exception as e:
                errors.put(e)

        # Start multiple threads
        threads = [threading.Thread(target=send_message, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Should have 10 successful sends and no errors
        assert results.qsize() == 10
        assert errors.qsize() == 0

        # All message IDs should be unique
        message_ids = []
        while not results.empty():
            message_ids.append(results.get())

        assert len(set(message_ids)) == 10


class TestKafkaPublisherErrorHandling:
    """Test error handling scenarios."""

    def test_producer_creation_failure(self, mocker):
        """Test handling of producer creation failure."""
        mock_producer = mocker.patch("cezzis_kafka.kafka_publisher.Producer")
        mock_producer.side_effect = Exception("Failed to connect to brokers")

        settings = KafkaPublisherSettings(bootstrap_servers="invalid:9092")

        with pytest.raises(Exception, match="Failed to connect to brokers"):
            KafkaPublisher(settings)

    def test_delivery_handler_creation_failure(self, mocker):
        """Test handling of delivery handler creation failure."""
        mocker.patch("cezzis_kafka.kafka_publisher.Producer")  # Success
        mock_handler = mocker.patch("cezzis_kafka.kafka_publisher.DeliveryHandler")
        mock_handler.side_effect = Exception("Handler creation failed")

        settings = KafkaPublisherSettings(bootstrap_servers="localhost:9092")

        with pytest.raises(Exception, match="Handler creation failed"):
            KafkaPublisher(settings)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
