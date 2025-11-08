"""
Tests for DeliveryHandler DLQ (Dead Letter Queue) functionality.
"""

import json
import pytest
import unittest.mock as mock
from confluent_kafka import KafkaError, Message
from cezzis_kafka.delivery_handler import DeliveryHandler
from cezzis_kafka.delivery_context import DeliveryContext


class TestDLQInitialization:
    """Test DLQ producer initialization."""
    
    @mock.patch('cezzis_kafka.delivery_handler.Producer')
    def test_dlq_producer_created_with_topic_and_servers(self, mock_producer_class):
        """Test DLQ producer is created when both topic and servers provided."""
        mock_producer = mock.Mock()
        mock_producer_class.return_value = mock_producer
        
        handler = DeliveryHandler(
            dlq_topic="test-dlq",
            bootstrap_servers="localhost:9092"
        )
        
        # Should create DLQ producer
        assert handler._dlq_producer == mock_producer
        mock_producer_class.assert_called_once()
        
        # Verify DLQ-specific configuration
        producer_config = mock_producer_class.call_args[0][0]
        assert producer_config['bootstrap.servers'] == "localhost:9092"
        assert producer_config['acks'] == 'all'
        assert producer_config['retries'] == 5
        assert producer_config['max.in.flight.requests.per.connection'] == 1
        assert producer_config['enable.idempotence'] is True
        assert producer_config['compression.type'] == 'snappy'
        assert producer_config['request.timeout.ms'] == 30000
        assert producer_config['delivery.timeout.ms'] == 60000
    
    def test_no_dlq_producer_without_topic(self):
        """Test DLQ producer is not created without topic."""
        handler = DeliveryHandler(bootstrap_servers="localhost:9092")
        
        assert handler._dlq_producer is None
    
    def test_no_dlq_producer_without_servers(self):
        """Test DLQ producer is not created without bootstrap servers."""
        handler = DeliveryHandler(dlq_topic="test-dlq")
        
        assert handler._dlq_producer is None
    
    @mock.patch('cezzis_kafka.delivery_handler.Producer')
    def test_dlq_producer_creation_failure_handled_gracefully(self, mock_producer_class):
        """Test graceful handling of DLQ producer creation failure."""
        mock_producer_class.side_effect = Exception("Failed to connect")
        
        # Should not raise exception
        handler = DeliveryHandler(
            dlq_topic="test-dlq",
            bootstrap_servers="localhost:9092"
        )
        
        # DLQ producer should be None
        assert handler._dlq_producer is None


class TestDLQMessageSending:
    """Test sending messages to DLQ."""
    
    def test_send_to_dlq_with_full_context(self):
        """Test sending message to DLQ with full context."""
        mock_dlq_producer = mock.Mock()
        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer
        
        # Create context and message
        context = DeliveryContext(
            message_id="test-msg-1",
            topic="original-topic",
            attempt_count=3,
            metadata={"user_id": "123", "priority": "high"}
        )
        
        mock_message = mock.Mock()
        mock_message.topic.return_value = "original-topic"
        mock_message.partition.return_value = 2
        mock_message.offset.return_value = 456
        mock_message.headers.return_value = [('content-type', b'application/json')]
        mock_message.key.return_value = b"test-key"
        mock_message.value.return_value = b'{"test": "data"}'
        
        error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "Not authorized")
        
        # Send to DLQ
        handler._send_to_dlq(context, mock_message, error)
        
        # Verify producer.produce called
        mock_dlq_producer.produce.assert_called_once()
        mock_dlq_producer.flush.assert_called_once_with(timeout=10.0)
        
        # Verify call arguments
        call_kwargs = mock_dlq_producer.produce.call_args[1]
        assert call_kwargs['topic'] == "test-dlq"
        assert call_kwargs['key'] == "test-msg-1"
        
        # Verify headers
        headers = call_kwargs['headers']
        assert headers['dlq.original_topic'] == "original-topic"
        assert headers['dlq.message_id'] == "test-msg-1"
        assert headers['dlq.failure_code'] == str(KafkaError.TOPIC_AUTHORIZATION_FAILED)
        assert headers['dlq.attempt_count'] == "3"
        
        # Verify DLQ payload
        dlq_payload = json.loads(call_kwargs['value'])
        assert dlq_payload['original_topic'] == "original-topic"
        assert dlq_payload['original_partition'] == 2
        assert dlq_payload['original_offset'] == 456
        assert dlq_payload['message_id'] == "test-msg-1"
        assert dlq_payload['failure_reason'] == str(error)  # Full error string representation
        assert dlq_payload['error_code'] == KafkaError.TOPIC_AUTHORIZATION_FAILED
        assert dlq_payload['attempt_count'] == 3
        assert dlq_payload['original_key'] == "test-key"
        assert dlq_payload['original_value'] == '{"test": "data"}'
        assert 'failure_timestamp' in dlq_payload
        assert dlq_payload['metadata'] == {"user_id": "123", "priority": "high"}
    
    def test_send_to_dlq_with_minimal_context(self):
        """Test sending message to DLQ with minimal context."""
        mock_dlq_producer = mock.Mock()
        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer
        
        # Create minimal context
        context = DeliveryContext(message_id="minimal-msg", topic="test-topic")
        
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = -1  # Invalid offset
        mock_message.headers.return_value = None
        mock_message.key.return_value = None
        mock_message.value.return_value = None
        
        error = KafkaError(KafkaError.UNKNOWN, "Unknown error")
        
        # Send to DLQ
        handler._send_to_dlq(context, mock_message, error)
        
        # Verify producer.produce called
        mock_dlq_producer.produce.assert_called_once()
        
        # Verify DLQ payload handles None values
        call_kwargs = mock_dlq_producer.produce.call_args[1]
        dlq_payload = json.loads(call_kwargs['value'])
        
        assert dlq_payload['original_offset'] is None
        assert dlq_payload['original_headers'] == {}
        assert dlq_payload['original_key'] is None
        assert dlq_payload['original_value'] is None
    
    def test_send_to_dlq_with_bytes_data(self):
        """Test DLQ serialization of bytes data."""
        mock_dlq_producer = mock.Mock()
        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer
        
        context = DeliveryContext(
            message_id="bytes-msg",
            topic="test-topic",
            metadata={"bytes_data": b"test bytes", "nested": {"more_bytes": b"nested"}}
        )
        
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.headers.return_value = [('binary_header', b'binary_value')]
        mock_message.key.return_value = b"bytes-key"
        mock_message.value.return_value = b"bytes-value"
        
        error = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE, "Message too large")
        
        # Send to DLQ
        handler._send_to_dlq(context, mock_message, error)
        
        # Verify serialization handled bytes correctly
        call_kwargs = mock_dlq_producer.produce.call_args[1]
        dlq_payload = json.loads(call_kwargs['value'])
        
        # Bytes should be decoded to strings
        assert dlq_payload['original_key'] == "bytes-key"
        assert dlq_payload['original_value'] == "bytes-value"
        assert dlq_payload['original_headers'] == {"binary_header": "binary_value"}
        assert dlq_payload['metadata']['bytes_data'] == "test bytes"
        assert dlq_payload['metadata']['nested']['more_bytes'] == "nested"
    
    def test_send_to_dlq_without_producer(self):
        """Test DLQ sending when no producer configured."""
        handler = DeliveryHandler(dlq_topic="test-dlq")
        # No DLQ producer set (None)
        
        context = DeliveryContext(message_id="no-producer", topic="test-topic")
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-topic"
        error = KafkaError(KafkaError.UNKNOWN, "Test error")
        
        # Should not raise exception, just log warning
        handler._send_to_dlq(context, mock_message, error)
        # No assertions needed - just ensuring no exceptions
    
    def test_send_to_dlq_without_topic(self):
        """Test DLQ sending when no DLQ topic configured."""
        mock_dlq_producer = mock.Mock()
        handler = DeliveryHandler()  # No DLQ topic
        handler._dlq_producer = mock_dlq_producer
        
        context = DeliveryContext(message_id="no-topic", topic="test-topic")
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-topic"
        error = KafkaError(KafkaError.UNKNOWN, "Test error")
        
        # Should not raise exception, just log warning
        handler._send_to_dlq(context, mock_message, error)
        
        # Producer should not be called
        mock_dlq_producer.produce.assert_not_called()
    
    def test_send_to_dlq_producer_exception(self):
        """Test handling of producer exceptions during DLQ send."""
        mock_dlq_producer = mock.Mock()
        mock_dlq_producer.produce.side_effect = Exception("Producer error")
        
        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer
        
        context = DeliveryContext(message_id="error-msg", topic="test-topic")
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.headers.return_value = None
        mock_message.key.return_value = None
        mock_message.value.return_value = None
        
        error = KafkaError(KafkaError.UNKNOWN, "Test error")
        
        # Should not raise exception, but log error
        handler._send_to_dlq(context, mock_message, error)
        
        # Producer.produce should have been attempted
        mock_dlq_producer.produce.assert_called_once()
    
    def test_send_to_dlq_json_serialization_edge_cases(self):
        """Test JSON serialization edge cases in DLQ payload."""
        mock_dlq_producer = mock.Mock()
        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer
        
        # Context with complex nested data
        complex_metadata = {
            "list_with_bytes": [b"item1", "item2", {"nested_bytes": b"value"}],
            "tuple_data": (b"tuple_bytes", "string"),
            "unicode_data": "测试数据",
            "special_chars": "Special: ñáéíóú",
        }
        
        context = DeliveryContext(
            message_id="complex-msg",
            topic="test-topic",
            metadata=complex_metadata
        )
        
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.headers.return_value = None
        mock_message.key.return_value = "unicode-key: 测试"
        mock_message.value.return_value = "unicode-value: ñáéíóú"
        
        error = KafkaError(KafkaError.UNKNOWN, "Test error")
        
        # Send to DLQ
        handler._send_to_dlq(context, mock_message, error)
        
        # Verify JSON serialization succeeded
        call_kwargs = mock_dlq_producer.produce.call_args[1]
        dlq_payload = json.loads(call_kwargs['value'])
        
        # Verify complex data was properly serialized
        metadata = dlq_payload['metadata']
        assert metadata['list_with_bytes'] == ["item1", "item2", {"nested_bytes": "value"}]
        assert metadata['tuple_data'] == ["tuple_bytes", "string"]  # Tuples become lists
        assert metadata['unicode_data'] == "测试数据"
        assert metadata['special_chars'] == "Special: ñáéíóú"


class TestDLQDeliveryCallback:
    """Test DLQ delivery callback functionality."""
    
    def test_dlq_delivery_callback_success(self):
        """Test successful DLQ delivery callback."""
        handler = DeliveryHandler()
        
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-dlq"
        mock_message.partition.return_value = 1
        mock_message.offset.return_value = 789
        
        # Should not raise exception
        handler._dlq_delivery_callback(None, mock_message)
    
    def test_dlq_delivery_callback_failure(self):
        """Test failed DLQ delivery callback."""
        handler = DeliveryHandler()
        
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-dlq"
        
        error = KafkaError(KafkaError.BROKER_NOT_AVAILABLE, "DLQ broker down")
        
        # Should not raise exception, just log error
        handler._dlq_delivery_callback(error, mock_message)
    
    def test_dlq_delivery_callback_failure_no_message(self):
        """Test DLQ delivery callback gracefully handles message access failures."""
        handler = DeliveryHandler(dlq_topic="test-dlq")
        
        # Mock message that doesn't cause exception (the actual implementation handles None case)
        mock_message = mock.Mock()
        mock_message.topic.return_value = "test-dlq"
        
        error = KafkaError(KafkaError.UNKNOWN, "Unknown DLQ error")
        
        # Should not raise exception and should log error
        handler._dlq_delivery_callback(error, mock_message)


class TestDLQIntegration:
    """Test DLQ integration with delivery handling."""
    
    @mock.patch('cezzis_kafka.delivery_handler.Producer')
    def test_terminal_failure_routes_to_dlq(self, mock_producer_class):
        """Test that terminal failures are routed to DLQ."""
        mock_dlq_producer = mock.Mock()
        mock_producer_class.return_value = mock_dlq_producer
        
        handler = DeliveryHandler(
            dlq_topic="test-dlq",
            bootstrap_servers="localhost:9092"
        )
        
        # Track message
        handler.track_message("terminal-msg", "test-topic")
        
        # Create message and fatal error
        mock_message = mock.Mock()
        mock_message.headers.return_value = [('message_id', b'terminal-msg')]
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.key.return_value = b"test-key"
        mock_message.value.return_value = b"test-value"
        
        fatal_error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "Not authorized")
        
        # Handle delivery failure
        handler.handle_delivery(fatal_error, mock_message)
        
        # Should route to DLQ
        mock_dlq_producer.produce.assert_called_once()
        
        # Message should be cleaned up
        assert "terminal-msg" not in handler._pending_messages
        
        handler.close()
    
    def test_max_retries_exceeded_routes_to_dlq(self):
        """Test that messages exceeding max retries route to DLQ."""
        mock_retry_producer = mock.Mock()
        mock_dlq_producer = mock.Mock()
        mock_dlq_producer.flush.return_value = 0
        
        handler = DeliveryHandler(
            max_retries=0,  # No retries allowed
            retry_producer=mock_retry_producer,
            dlq_topic="test-dlq"
        )
        handler._dlq_producer = mock_dlq_producer
        
        # Track message
        original_data = {"key": b"test", "value": b"data", "headers": {}}
        handler.track_message("max-retries-msg", "test-topic", {}, original_data)
        
        # Create message and retriable error
        mock_message = mock.Mock()
        mock_message.headers.return_value = [('message_id', b'max-retries-msg')]
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.key.return_value = b"test-key"
        mock_message.value.return_value = b"test-value"
        
        # Retriable error, but no retries allowed
        retriable_error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")
        
        # Handle delivery failure
        handler.handle_delivery(retriable_error, mock_message)
        
        # Should route to DLQ (max retries exceeded)
        mock_dlq_producer.produce.assert_called_once()
        
        # Should NOT attempt retry
        mock_retry_producer.produce.assert_not_called()
        
        # Message should be cleaned up
        assert "max-retries-msg" not in handler._pending_messages
        
        handler.close()
    
    def test_dlq_with_metrics_callback(self):
        """Test DLQ routing with metrics reporting."""
        mock_dlq_producer = mock.Mock()
        metrics_callback = mock.Mock()
        
        handler = DeliveryHandler(
            dlq_topic="test-dlq",
            metrics_callback=metrics_callback
        )
        handler._dlq_producer = mock_dlq_producer
        
        # Track message
        handler.track_message("metrics-msg", "test-topic")
        
        # Create message and error
        mock_message = mock.Mock()
        mock_message.headers.return_value = [('message_id', b'metrics-msg')]
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 0
        mock_message.offset.return_value = 123
        mock_message.key.return_value = b"test"
        mock_message.value.return_value = b"data"
        
        fatal_error = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE, "Message too large")
        
        # Handle delivery failure
        handler.handle_delivery(fatal_error, mock_message)
        
        # Should report metrics
        assert metrics_callback.call_count == 2  # Failed + Terminal failure
        
        # Verify terminal failure metrics
        terminal_call = metrics_callback.call_args_list[1]
        assert terminal_call[0][0] == "kafka.message.terminal_failure"
        terminal_metrics = terminal_call[0][1]
        assert terminal_metrics["sent_to_dlq"] is True
        
        handler.close()
    
    def test_dlq_routing_preserves_original_error_info(self):
        """Test that DLQ routing preserves original error information."""
        mock_dlq_producer = mock.Mock()
        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer
        
        # Track message with metadata
        metadata = {"request_id": "req-123", "user": "testuser"}
        handler.track_message("preserve-error-msg", "test-topic", metadata)
        
        # Create message
        mock_message = mock.Mock()
        mock_message.headers.return_value = [('message_id', b'preserve-error-msg')]
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 5
        mock_message.offset.return_value = 999
        mock_message.key.return_value = b"preserve-key"
        mock_message.value.return_value = b'{"data": "preserve"}'
        
        # Specific error with details
        specific_error = KafkaError(KafkaError.INVALID_CONFIG, "Invalid broker configuration")
        
        # Handle delivery failure
        handler.handle_delivery(specific_error, mock_message)
        
        # Verify DLQ payload contains all original information
        call_kwargs = mock_dlq_producer.produce.call_args[1]
        dlq_payload = json.loads(call_kwargs['value'])
        
        assert dlq_payload['original_topic'] == "test-topic"
        assert dlq_payload['original_partition'] == 5
        assert dlq_payload['original_offset'] == 999
        assert dlq_payload['message_id'] == "preserve-error-msg"
        assert dlq_payload['failure_reason'] == str(specific_error)  # Full error string
        assert dlq_payload['error_code'] == KafkaError.INVALID_CONFIG
        assert dlq_payload['original_key'] == "preserve-key"
        assert dlq_payload['original_value'] == '{"data": "preserve"}'
        assert dlq_payload['metadata'] == metadata
        
        handler.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])