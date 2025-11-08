"""
Tests for basic DeliveryHandler functionality.
"""

import unittest.mock as mock

import pytest
from confluent_kafka import KafkaError, Message

from cezzis_kafka.delivery_context import DeliveryContext
from cezzis_kafka.delivery_handler import DeliveryHandler
from cezzis_kafka.delivery_status import DeliveryStatus


class TestDeliveryHandlerInit:
    """Test DeliveryHandler initialization."""

    def test_init_with_defaults(self):
        """Test initialization with default values."""
        handler = DeliveryHandler()

        assert handler.max_retries == 3
        assert handler.retry_backoff_ms == 1000
        assert handler.dlq_topic is None
        assert handler.metrics_callback is None
        assert handler._pending_messages == {}
        assert handler._retry_producer is None
        assert handler._retry_timers == {}
        assert handler._shutdown is False
        assert handler._dlq_producer is None

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        metrics_callback = mock.Mock()
        retry_producer = mock.Mock()

        handler = DeliveryHandler(
            max_retries=5,
            retry_backoff_ms=2000,
            dlq_topic="test-dlq",
            metrics_callback=metrics_callback,
            retry_producer=retry_producer,
        )

        assert handler.max_retries == 5
        assert handler.retry_backoff_ms == 2000
        assert handler.dlq_topic == "test-dlq"
        assert handler.metrics_callback == metrics_callback
        assert handler._retry_producer == retry_producer
        assert handler._shutdown is False

    @mock.patch("cezzis_kafka.delivery_handler.Producer")
    def test_init_with_dlq_producer(self, mock_producer_class):
        """Test initialization with DLQ producer creation."""
        mock_producer = mock.Mock()
        mock_producer_class.return_value = mock_producer

        handler = DeliveryHandler(dlq_topic="test-dlq", bootstrap_servers="localhost:9092")

        # Should create DLQ producer
        assert handler._dlq_producer == mock_producer
        mock_producer_class.assert_called_once()

        # Verify producer config
        producer_config = mock_producer_class.call_args[0][0]
        assert producer_config["bootstrap.servers"] == "localhost:9092"
        assert producer_config["acks"] == "all"
        assert producer_config["retries"] == 5

    @mock.patch("cezzis_kafka.delivery_handler.Producer")
    def test_init_dlq_producer_failure(self, mock_producer_class):
        """Test graceful handling of DLQ producer creation failure."""
        mock_producer_class.side_effect = Exception("Connection failed")

        # Should not raise exception
        handler = DeliveryHandler(dlq_topic="test-dlq", bootstrap_servers="localhost:9092")

        # DLQ producer should be None
        assert handler._dlq_producer is None


class TestMessageTracking:
    """Test message tracking functionality."""

    def test_track_message_basic(self):
        """Test basic message tracking."""
        handler = DeliveryHandler()

        handler.track_message("msg-1", "test-topic")

        assert "msg-1" in handler._pending_messages
        context = handler._pending_messages["msg-1"]
        assert isinstance(context, DeliveryContext)
        assert context.message_id == "msg-1"
        assert context.topic == "test-topic"
        assert context.attempt_count == 0
        assert context.metadata == {}

    def test_track_message_with_metadata(self):
        """Test message tracking with metadata."""
        handler = DeliveryHandler()
        metadata = {"user_id": "123", "priority": "high"}

        handler.track_message("msg-1", "test-topic", metadata)

        context = handler._pending_messages["msg-1"]
        assert context.metadata == metadata

    def test_track_message_with_original_data(self):
        """Test message tracking with original message data."""
        handler = DeliveryHandler()
        original_data = {"key": b"test-key", "value": b"test-value", "headers": {"content-type": "application/json"}}

        handler.track_message("msg-1", "test-topic", {}, original_data)

        context = handler._pending_messages["msg-1"]
        assert "_original_message" in context.metadata
        assert context.metadata["_original_message"] == original_data

    def test_remove_pending_message(self):
        """Test removing pending messages."""
        handler = DeliveryHandler()

        handler.track_message("msg-1", "test-topic")
        assert "msg-1" in handler._pending_messages

        handler.remove_pending_message("msg-1")
        assert "msg-1" not in handler._pending_messages

    def test_remove_nonexistent_message(self):
        """Test removing non-existent message doesn't raise error."""
        handler = DeliveryHandler()

        # Should not raise exception
        handler.remove_pending_message("nonexistent")


class TestMessageIdExtraction:
    """Test message ID extraction from Kafka messages."""

    def test_extract_from_headers(self):
        """Test extracting message ID from headers."""
        handler = DeliveryHandler()
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"test-msg-123")]

        message_id = handler._extract_message_id(mock_message)

        assert message_id == "test-msg-123"

    def test_extract_from_headers_string_value(self):
        """Test extracting message ID from headers with string value."""
        handler = DeliveryHandler()
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", "test-msg-456")]

        message_id = handler._extract_message_id(mock_message)

        assert message_id == "test-msg-456"

    def test_extract_from_key_fallback(self):
        """Test fallback to message key when no message_id header."""
        handler = DeliveryHandler()
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("other_header", b"value")]
        mock_message.key.return_value = b"fallback-key"

        message_id = handler._extract_message_id(mock_message)

        assert message_id == "fallback-key"

    def test_extract_generated_fallback(self):
        """Test generated message ID when no headers or key."""
        handler = DeliveryHandler()
        mock_message = mock.Mock()
        mock_message.headers.return_value = None
        mock_message.key.return_value = None
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 2
        mock_message.offset.return_value = 123

        message_id = handler._extract_message_id(mock_message)

        assert message_id == "test-topic_2_123"

    def test_extract_no_headers(self):
        """Test extracting message ID when headers is None."""
        handler = DeliveryHandler()
        mock_message = mock.Mock()
        mock_message.headers.return_value = None
        mock_message.key.return_value = b"key-value"

        message_id = handler._extract_message_id(mock_message)

        assert message_id == "key-value"


class TestBasicDeliveryHandling:
    """Test basic delivery handling functionality."""

    def test_handle_delivery_unknown_message(self):
        """Test handling delivery for unknown message."""
        handler = DeliveryHandler()
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"unknown-msg")]
        mock_message.topic.return_value = "test-topic"

        # Should not raise exception
        handler.handle_delivery(None, mock_message)

        # Message should not be tracked
        assert "unknown-msg" not in handler._pending_messages

    def test_handle_successful_delivery_cleanup(self):
        """Test that successful delivery cleans up tracking."""
        handler = DeliveryHandler()

        # Track message first
        handler.track_message("msg-1", "test-topic")
        assert "msg-1" in handler._pending_messages

        # Create mock message
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123

        # Handle successful delivery
        handler.handle_delivery(None, mock_message)

        # Should clean up tracking
        assert "msg-1" not in handler._pending_messages

    def test_handle_failed_delivery_with_retry(self):
        """Test failed delivery that should be retried."""
        mock_producer = mock.Mock()
        handler = DeliveryHandler(retry_producer=mock_producer)

        # Track message
        original_data = {"key": b"test", "value": b"data", "headers": {}}
        handler.track_message("msg-1", "test-topic", {}, original_data)

        # Create mock message and error
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"

        error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")

        # Handle failed delivery
        handler.handle_delivery(error, mock_message)

        # Should still be tracked (retry scheduled)
        assert "msg-1" in handler._pending_messages
        # Attempt count should be incremented
        context = handler._pending_messages["msg-1"]
        assert context.attempt_count == 1

    def test_handle_failed_delivery_terminal(self):
        """Test failed delivery that is terminal."""
        handler = DeliveryHandler()

        # Track message
        handler.track_message("msg-1", "test-topic")

        # Create mock message and fatal error
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"

        error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "Not authorized")

        # Handle failed delivery
        handler.handle_delivery(error, mock_message)

        # Should be cleaned up (terminal failure)
        assert "msg-1" not in handler._pending_messages


class TestMetricsReporting:
    """Test metrics callback functionality."""

    def test_successful_delivery_metrics(self):
        """Test metrics reporting for successful delivery."""
        metrics_callback = mock.Mock()
        handler = DeliveryHandler(metrics_callback=metrics_callback)

        # Track and deliver message successfully
        handler.track_message("msg-1", "test-topic")

        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123

        handler.handle_delivery(None, mock_message)

        # Should call metrics callback for successful delivery
        metrics_callback.assert_called()
        call_args = metrics_callback.call_args
        assert call_args[0][0] == "kafka.message.delivered"
        metrics_data = call_args[0][1]
        assert metrics_data["status"] == "success"
        assert metrics_data["topic"] == "test-topic"
        assert "delivery_time_ms" in metrics_data

    def test_failed_delivery_metrics(self):
        """Test metrics reporting for failed delivery."""
        metrics_callback = mock.Mock()
        handler = DeliveryHandler(metrics_callback=metrics_callback)

        # Track message
        handler.track_message("msg-1", "test-topic")

        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"

        error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "Not authorized")
        handler.handle_delivery(error, mock_message)

        # Should call metrics callback for failed delivery and terminal failure
        assert metrics_callback.call_count == 2

        # First call for failure
        first_call = metrics_callback.call_args_list[0]
        assert first_call[0][0] == "kafka.message.failed"

        # Second call for terminal failure
        second_call = metrics_callback.call_args_list[1]
        assert second_call[0][0] == "kafka.message.terminal_failure"

    def test_no_metrics_callback(self):
        """Test that no errors occur when metrics callback is None."""
        handler = DeliveryHandler(metrics_callback=None)

        handler.track_message("msg-1", "test-topic")

        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123

        # Should not raise exception
        handler.handle_delivery(None, mock_message)


class TestUtilityMethods:
    """Test utility methods in DeliveryHandler."""

    def test_safe_decode_value_bytes(self):
        """Test safe decoding of bytes value."""
        handler = DeliveryHandler()

        result = handler._safe_decode_value(b"test message")
        assert result == "test message"

    def test_safe_decode_value_none(self):
        """Test safe decoding of None value."""
        handler = DeliveryHandler()

        result = handler._safe_decode_value(None)
        assert result is None

    def test_safe_decode_value_string(self):
        """Test safe decoding handles non-bytes input via str() fallback."""
        handler = DeliveryHandler()

        # The method will convert non-bytes through str()
        result = handler._safe_decode_value(123)  # type: ignore
        assert result == "123"

    def test_safe_decode_value_with_errors(self):
        """Test safe decoding with invalid UTF-8 bytes."""
        handler = DeliveryHandler()

        # Invalid UTF-8 sequence
        invalid_bytes = b"\xff\xfe"
        result = handler._safe_decode_value(invalid_bytes)

        # Should not raise exception and return some string
        assert isinstance(result, str)

    def test_serialize_for_dlq_bytes(self):
        """Test DLQ serialization of bytes."""
        handler = DeliveryHandler()

        result = handler._serialize_for_dlq(b"test bytes")
        assert result == "test bytes"

    def test_serialize_for_dlq_dict(self):
        """Test DLQ serialization of dict with mixed types."""
        handler = DeliveryHandler()

        input_dict = {"string_key": "value", "bytes_key": b"bytes_value", "int_key": 123}

        result = handler._serialize_for_dlq(input_dict)

        expected = {"string_key": "value", "bytes_key": "bytes_value", "int_key": 123}
        assert result == expected

    def test_serialize_for_dlq_list(self):
        """Test DLQ serialization of list with mixed types."""
        handler = DeliveryHandler()

        input_list = ["string", b"bytes", 123, {"nested": b"value"}]
        result = handler._serialize_for_dlq(input_list)

        expected = ["string", "bytes", 123, {"nested": "value"}]
        assert result == expected

    def test_serialize_for_dlq_nested(self):
        """Test DLQ serialization of nested structures."""
        handler = DeliveryHandler()

        complex_obj = {"level1": {"level2": [b"bytes_in_list", {"level3": b"deep_bytes"}]}}

        result = handler._serialize_for_dlq(complex_obj)

        expected = {"level1": {"level2": ["bytes_in_list", {"level3": "deep_bytes"}]}}
        assert result == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
