"""
Tests for DeliveryHandler error classification functionality.
"""

import unittest.mock as mock

import pytest
from confluent_kafka import KafkaError

from cezzis_kafka.delivery_handler import DeliveryHandler
from cezzis_kafka.delivery_status import DeliveryStatus


class TestErrorClassification:
    """Test error classification logic in DeliveryHandler."""

    def test_classify_network_errors_as_retriable(self):
        """Test that network errors are classified as retriable."""
        handler = DeliveryHandler()

        retriable_network_errors = [
            KafkaError.NETWORK_EXCEPTION,
            KafkaError.BROKER_NOT_AVAILABLE,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError.REQUEST_TIMED_OUT,
            KafkaError.NOT_ENOUGH_REPLICAS,
            KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        ]

        for error_code in retriable_network_errors:
            error = KafkaError(error_code, f"Test error {error_code}")
            status = handler._classify_error(error)
            assert status == DeliveryStatus.RETRIABLE_ERROR, f"Error {error_code} should be retriable"

    def test_classify_throttling_errors_as_retriable(self):
        """Test that throttling errors are classified as retriable."""
        handler = DeliveryHandler()

        throttling_errors = [
            KafkaError.THROTTLING_QUOTA_EXCEEDED,
        ]

        for error_code in throttling_errors:
            error = KafkaError(error_code, f"Test error {error_code}")
            status = handler._classify_error(error)
            assert status == DeliveryStatus.RETRIABLE_ERROR, f"Error {error_code} should be retriable"

    def test_classify_authorization_errors_as_fatal(self):
        """Test that authorization errors are classified as fatal."""
        handler = DeliveryHandler()

        fatal_auth_errors = [
            KafkaError.TOPIC_AUTHORIZATION_FAILED,
            KafkaError.CLUSTER_AUTHORIZATION_FAILED,
            KafkaError.INVALID_CONFIG,
            KafkaError.UNKNOWN_TOPIC_OR_PART,
        ]

        for error_code in fatal_auth_errors:
            error = KafkaError(error_code, f"Test error {error_code}")
            status = handler._classify_error(error)
            assert status == DeliveryStatus.FATAL_ERROR, f"Error {error_code} should be fatal"

    def test_classify_message_format_errors_as_fatal(self):
        """Test that message format errors are classified as fatal."""
        handler = DeliveryHandler()

        fatal_format_errors = [
            KafkaError.MSG_SIZE_TOO_LARGE,
            KafkaError.INVALID_MSG_SIZE,
            KafkaError.INVALID_MSG,
        ]

        for error_code in fatal_format_errors:
            error = KafkaError(error_code, f"Test error {error_code}")
            status = handler._classify_error(error)
            assert status == DeliveryStatus.FATAL_ERROR, f"Error {error_code} should be fatal"

    def test_classify_unknown_errors_as_retriable(self):
        """Test that unknown errors default to retriable."""
        handler = DeliveryHandler()

        # Use an error code that's not explicitly handled
        unknown_error = KafkaError(KafkaError.UNKNOWN, "Unknown error")
        status = handler._classify_error(unknown_error)

        assert status == DeliveryStatus.RETRIABLE_ERROR

    def test_classify_all_retriable_error_codes(self):
        """Test comprehensive list of retriable error codes."""
        handler = DeliveryHandler()

        # Test all explicitly retriable errors
        retriable_codes = [
            # Network/broker issues
            KafkaError.NETWORK_EXCEPTION,
            KafkaError.BROKER_NOT_AVAILABLE,
            KafkaError.LEADER_NOT_AVAILABLE,
            KafkaError.REQUEST_TIMED_OUT,
            KafkaError.NOT_ENOUGH_REPLICAS,
            KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            # Quota/throttling
            KafkaError.THROTTLING_QUOTA_EXCEEDED,
        ]

        for error_code in retriable_codes:
            error = KafkaError(error_code, "Test message")
            status = handler._classify_error(error)
            assert status == DeliveryStatus.RETRIABLE_ERROR, f"Code {error_code} should be retriable"

    def test_classify_all_fatal_error_codes(self):
        """Test comprehensive list of fatal error codes."""
        handler = DeliveryHandler()

        # Test all explicitly fatal errors
        fatal_codes = [
            # Authorization/configuration
            KafkaError.TOPIC_AUTHORIZATION_FAILED,
            KafkaError.CLUSTER_AUTHORIZATION_FAILED,
            KafkaError.INVALID_CONFIG,
            KafkaError.UNKNOWN_TOPIC_OR_PART,
            # Message format
            KafkaError.MSG_SIZE_TOO_LARGE,
            KafkaError.INVALID_MSG_SIZE,
            KafkaError.INVALID_MSG,
        ]

        for error_code in fatal_codes:
            error = KafkaError(error_code, "Test message")
            status = handler._classify_error(error)
            assert status == DeliveryStatus.FATAL_ERROR, f"Code {error_code} should be fatal"

    @pytest.mark.parametrize(
        "error_code,expected_status",
        [
            # Retriable errors
            (KafkaError.NETWORK_EXCEPTION, DeliveryStatus.RETRIABLE_ERROR),
            (KafkaError.BROKER_NOT_AVAILABLE, DeliveryStatus.RETRIABLE_ERROR),
            (KafkaError.LEADER_NOT_AVAILABLE, DeliveryStatus.RETRIABLE_ERROR),
            (KafkaError.REQUEST_TIMED_OUT, DeliveryStatus.RETRIABLE_ERROR),
            (KafkaError.NOT_ENOUGH_REPLICAS, DeliveryStatus.RETRIABLE_ERROR),
            (KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND, DeliveryStatus.RETRIABLE_ERROR),
            (KafkaError.THROTTLING_QUOTA_EXCEEDED, DeliveryStatus.RETRIABLE_ERROR),
            # Fatal errors
            (KafkaError.TOPIC_AUTHORIZATION_FAILED, DeliveryStatus.FATAL_ERROR),
            (KafkaError.CLUSTER_AUTHORIZATION_FAILED, DeliveryStatus.FATAL_ERROR),
            (KafkaError.INVALID_CONFIG, DeliveryStatus.FATAL_ERROR),
            (KafkaError.UNKNOWN_TOPIC_OR_PART, DeliveryStatus.FATAL_ERROR),
            (KafkaError.MSG_SIZE_TOO_LARGE, DeliveryStatus.FATAL_ERROR),
            (KafkaError.INVALID_MSG_SIZE, DeliveryStatus.FATAL_ERROR),
            (KafkaError.INVALID_MSG, DeliveryStatus.FATAL_ERROR),
            # Default to retriable
            (KafkaError.UNKNOWN, DeliveryStatus.RETRIABLE_ERROR),
        ],
    )
    def test_error_classification_matrix(self, error_code, expected_status):
        """Parametrized test for error classification matrix."""
        handler = DeliveryHandler()

        error = KafkaError(error_code, f"Test error for {error_code}")
        status = handler._classify_error(error)

        assert status == expected_status


class TestErrorClassificationIntegration:
    """Test error classification integration with delivery handling."""

    def test_retriable_error_final_failure(self):
        """Test that retriable errors are handled as final failures (after Kafka retries)."""
        handler = DeliveryHandler()

        # Track message
        handler.track_message("msg-1", "test-topic")

        # Create mock message
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"

        # Network error - final failure after Kafka's built-in retries
        network_error = KafkaError(KafkaError.NETWORK_EXCEPTION, "Network timeout")

        # Handle failed delivery (this represents final failure)
        handler.handle_delivery(network_error, mock_message)

        # Should clean up message (final failure)
        assert "msg-1" not in handler._pending_messages

    def test_fatal_error_triggers_terminal_handling(self):
        """Test that fatal errors trigger terminal handling."""
        handler = DeliveryHandler()

        # Track message
        handler.track_message("msg-1", "test-topic")

        # Create mock message
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"

        # Authorization error should be fatal
        auth_error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, "Not authorized")

        # Handle failed delivery
        handler.handle_delivery(auth_error, mock_message)

        # Should clean up message (terminal failure)
        assert "msg-1" not in handler._pending_messages

    def test_error_classification_with_metrics(self):
        """Test that error classification properly reports metrics."""
        metrics_callback = mock.Mock()
        handler = DeliveryHandler(metrics_callback=metrics_callback)

        # Track message
        handler.track_message("msg-1", "test-topic")

        # Create mock message
        mock_message = mock.Mock()
        mock_message.headers.return_value = [("message_id", b"msg-1")]
        mock_message.topic.return_value = "test-topic"

        # Test retriable error metrics (final failure)
        retriable_error = KafkaError(KafkaError.BROKER_NOT_AVAILABLE, "Broker down")
        handler.handle_delivery(retriable_error, mock_message)

        # Should report failure metrics with retriable status
        metrics_callback.assert_called()
        failure_call = metrics_callback.call_args
        assert failure_call[0][0] == "kafka.message.failed"
        failure_metrics = failure_call[0][1]
        assert failure_metrics["status"] == "retriable_error"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
