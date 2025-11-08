"""
Tests for DeliveryHandler shutdown and cleanup functionality.
"""

import threading
import time
import unittest.mock as mock

import pytest
from confluent_kafka import KafkaError, Message

from cezzis_kafka.delivery_handler import DeliveryHandler


class TestShutdownAndCleanup:
    """Test proper shutdown and cleanup of DeliveryHandler."""

    def test_close_cancels_retry_timers(self):
        """Test that close() cancels all active retry timers."""
        mock_producer = mock.Mock()
        handler = DeliveryHandler(retry_producer=mock_producer, retry_backoff_ms=100)

        # Track multiple messages
        for i in range(3):
            original_data = {"key": b"test", "value": b"data", "headers": {}}
            handler.track_message(f"msg-{i}", "test-topic", {}, original_data)

        # Create mock messages and schedule retries
        for i in range(3):
            mock_message = mock.Mock()
            mock_message.headers.return_value = [("message_id", f"msg-{i}".encode())]
            mock_message.topic.return_value = "test-topic"

            error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")
            handler.handle_delivery(error, mock_message)

        # Should have 3 active timers
        assert len(handler._retry_timers) == 3

        # Close handler
        handler.close()

        # All timers should be cancelled and cleared
        assert len(handler._retry_timers) == 0

        # Wait a bit to ensure retries don't execute
        time.sleep(0.15)

        # Producer should not have been called for retries after close
        assert mock_producer.produce.call_count == 0

    def test_close_shuts_down_dlq_producer(self):
        """Test that close() properly shuts down DLQ producer."""
        mock_dlq_producer = mock.Mock()
        mock_dlq_producer.flush.return_value = 0

        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer

        # Close handler
        handler.close()

        # DLQ producer should be flushed and set to None
        mock_dlq_producer.flush.assert_called_once_with(timeout=30.0)
        assert handler._dlq_producer is None

    def test_close_clears_pending_messages(self):
        """Test that close() clears all pending messages."""
        handler = DeliveryHandler()

        # Track several messages
        for i in range(5):
            handler.track_message(f"pending-{i}", "test-topic")

        assert len(handler._pending_messages) == 5

        # Close handler
        handler.close()

        # All pending messages should be cleared
        assert len(handler._pending_messages) == 0

    def test_close_sets_shutdown_flag(self):
        """Test that close() sets the shutdown flag."""
        handler = DeliveryHandler()

        assert handler._shutdown is False

        handler.close()

        assert handler._shutdown is True

    def test_shutdown_prevents_new_retries(self):
        """Test that shutdown flag prevents new retry scheduling."""
        mock_producer = mock.Mock()
        handler = DeliveryHandler(retry_producer=mock_producer)

        # Track message
        original_data = {"key": b"test", "value": b"data", "headers": {}}
        handler.track_message("shutdown-test", "test-topic", {}, original_data)

        # Shut down first
        handler.close()

        # Try to schedule retry after shutdown
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"shutdown-test")]
        mock_message.topic.return_value = "test-topic"

        error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")
        handler.handle_delivery(error, mock_message)

        # Should not schedule retry after shutdown
        assert len(handler._retry_timers) == 0
        mock_producer.produce.assert_not_called()

    def test_shutdown_skips_retry_execution(self):
        """Test that retry execution is skipped during shutdown."""
        mock_producer = mock.Mock()
        handler = DeliveryHandler(retry_producer=mock_producer, retry_backoff_ms=50)

        # Track message and schedule retry
        original_data = {"key": b"test", "value": b"data", "headers": {}}
        handler.track_message("exec-test", "test-topic", {}, original_data)

        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"exec-test")]
        mock_message.topic.return_value = "test-topic"

        error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")
        handler.handle_delivery(error, mock_message)

        # Should have scheduled retry
        assert len(handler._retry_timers) == 1

        # Shutdown before retry executes
        handler.close()

        # Wait for retry time to pass
        time.sleep(0.1)

        # Retry should not have executed
        mock_producer.produce.assert_not_called()

    def test_close_handles_timer_cancellation_errors(self):
        """Test that close() handles errors during timer cancellation gracefully."""
        handler = DeliveryHandler()

        # Create a mock timer that raises exception on cancel
        mock_timer = mock.Mock()
        mock_timer.cancel.side_effect = Exception("Timer error")
        handler._retry_timers["error-timer"] = mock_timer

        # Should not raise exception
        handler.close()

        # Timer should still be removed from dict
        assert "error-timer" not in handler._retry_timers

    def test_close_handles_dlq_producer_flush_errors(self):
        """Test that close() handles DLQ producer flush errors gracefully."""
        mock_dlq_producer = mock.Mock()
        mock_dlq_producer.flush.side_effect = Exception("Flush error")

        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer

        # Should not raise exception
        handler.close()

        # DLQ producer should still be set to None
        assert handler._dlq_producer is None

    def test_close_handles_dlq_producer_flush_timeout(self):
        """Test handling of DLQ producer flush with remaining messages."""
        mock_dlq_producer = mock.Mock()
        mock_dlq_producer.flush.return_value = 5  # 5 messages remaining

        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer

        # Should not raise exception, but log warning
        handler.close()

        mock_dlq_producer.flush.assert_called_once_with(timeout=30.0)
        assert handler._dlq_producer is None

    def test_multiple_close_calls_are_safe(self):
        """Test that multiple calls to close() are safe."""
        mock_dlq_producer = mock.Mock()
        mock_dlq_producer.flush.return_value = 0

        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer

        # Track some data
        handler.track_message("multi-close", "test-topic")

        # First close
        handler.close()
        assert handler._shutdown is True
        assert handler._dlq_producer is None
        assert len(handler._pending_messages) == 0

        # Second close should be safe
        handler.close()

        # Should only call flush once
        mock_dlq_producer.flush.assert_called_once()


class TestResourceManagement:
    """Test proper resource management during handler lifecycle."""

    def test_context_manager_pattern(self):
        """Test that handler can be used as context manager (conceptually)."""
        mock_dlq_producer = mock.Mock()
        mock_dlq_producer.flush.return_value = 0

        handler = DeliveryHandler(dlq_topic="test-dlq")
        handler._dlq_producer = mock_dlq_producer

        # Track some messages
        handler.track_message("ctx-1", "test-topic")
        handler.track_message("ctx-2", "test-topic")

        try:
            # Do some work with handler
            assert len(handler._pending_messages) == 2
            assert handler._dlq_producer is not None
        finally:
            # Always clean up
            handler.close()

        # Resources should be cleaned up
        assert handler._shutdown is True
        assert handler._dlq_producer is None
        assert len(handler._pending_messages) == 0

    def test_retry_timer_cleanup_with_concurrent_access(self):
        """Test retry timer cleanup with potential concurrent access."""
        mock_producer = mock.Mock()
        handler = DeliveryHandler(retry_producer=mock_producer, retry_backoff_ms=200)

        # Track message and start retry
        original_data = {"key": b"test", "value": b"data", "headers": {}}
        handler.track_message("concurrent", "test-topic", {}, original_data)

        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"concurrent")]
        mock_message.topic.return_value = "test-topic"

        error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")
        handler.handle_delivery(error, mock_message)

        # Timer should be scheduled
        assert len(handler._retry_timers) == 1

        # Close handler while timer might be executing
        handler.close()

        # Should clean up safely
        assert len(handler._retry_timers) == 0
        assert handler._shutdown is True

    def test_memory_cleanup_after_many_operations(self):
        """Test that memory is properly cleaned up after many operations."""
        handler = DeliveryHandler()

        # Track many messages
        for i in range(100):
            handler.track_message(f"mem-test-{i}", "test-topic")

        assert len(handler._pending_messages) == 100

        # Process some successful deliveries
        for i in range(50):
            mock_message = mock.Mock()
            mock_message.headers.return_value = [("message_id", f"mem-test-{i}".encode())]
            mock_message.topic.return_value = "test-topic"
            mock_message.partition.return_value = 0
            mock_message.offset.return_value = i

            handler.handle_delivery(None, mock_message)  # Successful delivery

        # Should have cleaned up successful deliveries
        assert len(handler._pending_messages) == 50

        # Close should clean up remaining
        handler.close()
        assert len(handler._pending_messages) == 0

    def test_cleanup_with_mixed_timer_states(self):
        """Test cleanup when timers are in various states."""
        mock_producer = mock.Mock()
        handler = DeliveryHandler(retry_producer=mock_producer, retry_backoff_ms=50)

        # Create timers in different states
        original_data = {"key": b"test", "value": b"data", "headers": {}}

        # Schedule some retries with different delays
        for i in range(3):
            handler.track_message(f"timer-{i}", "test-topic", {}, original_data)

            mock_message = mock.Mock()
            mock_message.headers.return_value = [("message_id", f"timer-{i}".encode())]
            mock_message.topic.return_value = "test-topic"

            error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")
            handler.handle_delivery(error, mock_message)

        # Should have multiple timers
        assert len(handler._retry_timers) == 3

        # Let some timers potentially start
        time.sleep(0.025)

        # Close should cancel all timers regardless of state
        handler.close()

        assert len(handler._retry_timers) == 0
        assert handler._shutdown is True

        # Wait to ensure no retries execute after close
        time.sleep(0.1)

        # No retries should have executed
        mock_producer.produce.assert_not_called()


class TestThreadSafety:
    """Test thread safety aspects of cleanup."""

    def test_concurrent_close_and_retry_scheduling(self):
        """Test concurrent close and retry scheduling operations."""
        mock_producer = mock.Mock()
        handler = DeliveryHandler(retry_producer=mock_producer, retry_backoff_ms=100)

        # Track message
        original_data = {"key": b"test", "value": b"data", "headers": {}}
        handler.track_message("thread-test", "test-topic", {}, original_data)

        def schedule_retry():
            """Function to schedule retry in thread."""
            mock_message = mock.Mock()
            mock_message.headers.return_value = [("message_id", b"thread-test")]
            mock_message.topic.return_value = "test-topic"

            error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")
            handler.handle_delivery(error, mock_message)

        def close_handler():
            """Function to close handler in thread."""
            time.sleep(0.01)  # Small delay
            handler.close()

        # Start both operations concurrently
        retry_thread = threading.Thread(target=schedule_retry)
        close_thread = threading.Thread(target=close_handler)

        retry_thread.start()
        close_thread.start()

        # Wait for both to complete
        retry_thread.join()
        close_thread.join()

        # Handler should be shut down
        assert handler._shutdown is True

        # No retries should execute after shutdown
        time.sleep(0.15)
        mock_producer.produce.assert_not_called()

    def test_concurrent_timer_execution_and_close(self):
        """Test concurrent timer execution and close operations."""
        mock_producer = mock.Mock()
        handler = DeliveryHandler(retry_producer=mock_producer, retry_backoff_ms=50)

        # Track message and schedule retry
        original_data = {"key": b"test", "value": b"data", "headers": {}}
        handler.track_message("exec-close", "test-topic", {}, original_data)

        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"exec-close")]
        mock_message.topic.return_value = "test-topic"

        error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network error")
        handler.handle_delivery(error, mock_message)

        # Close after a very short delay (during timer execution window)
        def delayed_close():
            time.sleep(0.025)  # Close while timer might be executing
            handler.close()

        close_thread = threading.Thread(target=delayed_close)
        close_thread.start()

        # Wait for close to complete
        close_thread.join()

        # Wait a bit longer for any potential retry execution
        time.sleep(0.1)

        # Verify clean shutdown
        assert handler._shutdown is True
        assert len(handler._retry_timers) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
