"""
Test for retry mechanism in DeliveryHandler.
"""

import threading
import time
import unittest.mock as mock

from confluent_kafka import KafkaError, Message

from cezzis_kafka.delivery_context import DeliveryContext
from cezzis_kafka.delivery_handler import DeliveryHandler
from cezzis_kafka.delivery_status import DeliveryStatus


def test_retry_scheduling():
    """Test that retry scheduling works with exponential backoff."""

    # Mock producer for retry attempts
    mock_producer = mock.Mock()

    # Create delivery handler with retry capability
    handler = DeliveryHandler(
        max_retries=2,
        retry_backoff_ms=100,  # Fast for testing
        retry_producer=mock_producer,
    )

    # Create mock message
    mock_message = mock.Mock()
    mock_message.topic.return_value = "test-topic"
    mock_message.partition.return_value = 0
    mock_message.offset.return_value = 123
    mock_message.headers.return_value = [("message_id", b"test-msg-1")]
    mock_message.key.return_value = b"test-key"
    mock_message.value.return_value = b"test-value"

    # Track the message with original data
    original_data = {
        "value": b"test-value",
        "key": b"test-key",
        "headers": {"message_id": "test-msg-1"},
        "topic": "test-topic",
    }

    handler.track_message("test-msg-1", "test-topic", {}, original_data)

    # Simulate retriable error
    retriable_error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")

    print("Testing retry mechanism...")

    # First delivery attempt fails
    handler.handle_delivery(retriable_error, mock_message)

    # Should have scheduled a retry
    assert "test-msg-1" in handler._retry_timers
    print("✓ Retry timer scheduled for message test-msg-1")

    # Wait for retry to execute (with some buffer time)
    time.sleep(0.2)

    # Should have called producer.produce for retry
    assert mock_producer.produce.called
    print("✓ Retry attempt executed via producer")

    # Verify retry headers were added
    call_args = mock_producer.produce.call_args
    retry_headers = call_args[1]["headers"]
    assert "retry_attempt" in retry_headers
    # The first retry should show attempt_count=1 (since it's incremented during scheduling)
    assert retry_headers["retry_attempt"] == "1"
    print("✓ Retry headers added correctly")

    handler.close()
    print("✓ Test completed successfully")


def test_terminal_failure_dlq():
    """Test that terminal failures go to DLQ without retry."""

    mock_dlq_producer = mock.Mock()
    # Fix the flush method to return a reasonable value
    mock_dlq_producer.flush.return_value = 0

    handler = DeliveryHandler(max_retries=2, dlq_topic="test-dlq", bootstrap_servers="localhost:9092")

    # Mock the DLQ producer
    handler._dlq_producer = mock_dlq_producer

    # Create mock message
    mock_message = mock.Mock()
    mock_message.topic.return_value = "test-topic"
    mock_message.partition.return_value = 0
    mock_message.offset.return_value = 123
    mock_message.headers.return_value = [("message_id", b"test-msg-2")]
    mock_message.key.return_value = b"test-key"
    mock_message.value.return_value = b"test-value"

    handler.track_message("test-msg-2", "test-topic")

    # Simulate fatal error (authorization failure)
    fatal_error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "Not authorized")

    print("\\nTesting DLQ routing for terminal failures...")

    # Handle fatal error
    handler.handle_delivery(fatal_error, mock_message)

    # Should NOT schedule retry
    assert "test-msg-2" not in handler._retry_timers
    print("✓ No retry scheduled for fatal error")

    # Should send to DLQ
    assert mock_dlq_producer.produce.called
    print("✓ Message sent to DLQ")

    # Verify DLQ payload contains error info
    call_args = mock_dlq_producer.produce.call_args
    dlq_key = call_args[1]["key"]
    dlq_headers = call_args[1]["headers"]

    assert dlq_key == "test-msg-2"
    assert "dlq.failure_code" in dlq_headers
    print("✓ DLQ message formatted correctly")

    handler.close()
    print("✓ DLQ test completed successfully")


def test_max_retries_exceeded():
    """Test that messages go to DLQ after max retries."""

    mock_producer = mock.Mock()
    mock_dlq_producer = mock.Mock()
    mock_dlq_producer.flush.return_value = 0

    handler = DeliveryHandler(
        max_retries=1,  # Only one retry
        retry_backoff_ms=50,
        dlq_topic="test-dlq",
        retry_producer=mock_producer,
    )

    handler._dlq_producer = mock_dlq_producer

    mock_message = mock.Mock()
    mock_message.topic.return_value = "test-topic"
    mock_message.partition.return_value = 0
    mock_message.offset.return_value = 123
    mock_message.headers.return_value = [("message_id", b"test-msg-3")]
    mock_message.key.return_value = b"test-key"
    mock_message.value.return_value = b"test-value"

    # Track with original data for retries
    original_data = {
        "value": b"test-value",
        "key": b"test-key",
        "headers": {"message_id": "test-msg-3"},
        "topic": "test-topic",
    }

    handler.track_message("test-msg-3", "test-topic", {}, original_data)

    retriable_error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")

    print("\\nTesting max retries exceeded behavior...")

    # First failure - should retry (attempt_count goes from 0 to 1)
    handler.handle_delivery(retriable_error, mock_message)
    time.sleep(0.1)  # Wait for retry

    print("✓ First retry scheduled and executed")

    # Need to track the message again since the retry creates a new delivery attempt
    # but the context should still exist
    context = handler._pending_messages.get("test-msg-3")
    assert context is not None, "Message context should still exist"

    # Second failure after retry - should go to DLQ
    handler.handle_delivery(retriable_error, mock_message)

    # Should send to DLQ (no more retries)
    assert mock_dlq_producer.produce.called
    print("✓ Message sent to DLQ after max retries exceeded")

    handler.close()
    print("✓ Max retries test completed successfully")


if __name__ == "__main__":
    test_retry_scheduling()
    test_terminal_failure_dlq()
    test_max_retries_exceeded()
    print("\\n🎉 All retry mechanism tests passed!")
