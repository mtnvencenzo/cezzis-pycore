"""
Tests for DeliveryContext class.
"""

import pytest
import time
from cezzis_kafka.delivery_context import DeliveryContext


class TestDeliveryContext:
    """Test suite for DeliveryContext class."""
    
    def test_init_with_required_parameters(self):
        """Test initialization with only required parameters."""
        context = DeliveryContext(
            message_id="test-msg-123",
            topic="test-topic"
        )
        
        assert context.message_id == "test-msg-123"
        assert context.topic == "test-topic"
        assert context.attempt_count == 0  # Default value
        assert isinstance(context.original_timestamp, float)
        assert context.metadata == {}
    
    def test_init_with_all_parameters(self):
        """Test initialization with all parameters."""
        metadata = {"correlation_id": "abc123", "source": "test"}
        timestamp = time.time()
        
        context = DeliveryContext(
            message_id="custom-msg-456",
            topic="custom-topic",
            attempt_count=3,
            original_timestamp=timestamp,
            metadata=metadata
        )
        
        assert context.message_id == "custom-msg-456"
        assert context.topic == "custom-topic"
        assert context.attempt_count == 3
        assert context.original_timestamp == timestamp
        assert context.metadata == metadata
    
    def test_default_attempt_count(self):
        """Test that attempt_count defaults to 0."""
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic"
        )
        
        assert context.attempt_count == 0
    
    def test_default_timestamp(self):
        """Test that original_timestamp is set to current time by default."""
        before_creation = time.time()
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic"
        )
        after_creation = time.time()
        
        # Should be between the before and after times
        assert before_creation <= context.original_timestamp <= after_creation
    
    def test_default_metadata(self):
        """Test that metadata defaults to empty dict."""
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic"
        )
        
        assert context.metadata == {}
        assert isinstance(context.metadata, dict)
    
    def test_metadata_independence(self):
        """Test that metadata is independent across instances."""
        context1 = DeliveryContext(
            message_id="msg-1",
            topic="topic-1"
        )
        context2 = DeliveryContext(
            message_id="msg-2", 
            topic="topic-2"
        )
        
        # Modify metadata in one instance
        context1.metadata["test"] = "value1"
        context2.metadata["test"] = "value2"
        
        # Should not affect the other
        assert context1.metadata["test"] == "value1"
        assert context2.metadata["test"] == "value2"
    
    def test_attempt_count_modification(self):
        """Test that attempt_count can be modified."""
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic"
        )
        
        # Initially 0
        assert context.attempt_count == 0
        
        # Can be incremented
        context.attempt_count += 1
        assert context.attempt_count == 1
        
        # Can be set to any value
        context.attempt_count = 5
        assert context.attempt_count == 5
    
    def test_string_representation(self):
        """Test string representation of DeliveryContext."""
        context = DeliveryContext(
            message_id="test-msg-789",
            topic="log-events",
            attempt_count=2,
            metadata={"key": "value"}
        )
        
        str_repr = str(context)
        
        # Should contain key information
        assert "test-msg-789" in str_repr
        assert "log-events" in str_repr
        assert "2" in str_repr  # attempt_count
    
    def test_equality(self):
        """Test equality comparison between DeliveryContext instances."""
        timestamp = time.time()
        metadata = {"test": "data"}
        
        context1 = DeliveryContext(
            message_id="same-msg",
            topic="same-topic",
            attempt_count=1,
            original_timestamp=timestamp,
            metadata=metadata
        )
        
        context2 = DeliveryContext(
            message_id="same-msg",
            topic="same-topic", 
            attempt_count=1,
            original_timestamp=timestamp,
            metadata=metadata
        )
        
        # Should be equal with same values
        assert context1 == context2
        
        # Different message_id should make them unequal
        context3 = DeliveryContext(
            message_id="different-msg",
            topic="same-topic",
            attempt_count=1,
            original_timestamp=timestamp,
            metadata=metadata
        )
        
        assert context1 != context3
    
    def test_metadata_types(self):
        """Test various metadata types."""
        metadata = {
            "string_val": "test",
            "int_val": 123,
            "float_val": 45.67,
            "bool_val": True,
            "list_val": [1, 2, 3],
            "dict_val": {"nested": "value"}
        }
        
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic",
            metadata=metadata
        )
        
        assert context.metadata["string_val"] == "test"
        assert context.metadata["int_val"] == 123
        assert context.metadata["float_val"] == 45.67
        assert context.metadata["bool_val"] is True
        assert context.metadata["list_val"] == [1, 2, 3]
        assert context.metadata["dict_val"] == {"nested": "value"}
    
    @pytest.mark.parametrize("message_id,topic", [
        ("msg-1", "topic-1"),
        ("very-long-message-id-with-many-characters", "short"),
        ("123", "numbers-456-topic"),
        ("msg_with_underscores", "topic-with-dashes"),
        ("", "empty-message-id"),  # Edge case
        ("normal-msg", ""),  # Edge case
    ])
    def test_various_id_topic_combinations(self, message_id, topic):
        """Test various combinations of message_id and topic."""
        context = DeliveryContext(
            message_id=message_id,
            topic=topic
        )
        
        assert context.message_id == message_id
        assert context.topic == topic
    
    def test_timestamp_precision(self):
        """Test that timestamp has reasonable precision."""
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic"
        )
        
        # Should be a float with reasonable precision
        assert isinstance(context.original_timestamp, float)
        assert context.original_timestamp > 1600000000  # After 2020
        assert context.original_timestamp < 2000000000  # Before 2033
    
    def test_custom_timestamp(self):
        """Test setting custom timestamp."""
        custom_timestamp = 1699459200.123  # Nov 8, 2023 with fractional seconds
        
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic",
            original_timestamp=custom_timestamp
        )
        
        assert context.original_timestamp == custom_timestamp
    
    def test_dataclass_features(self):
        """Test that DeliveryContext behaves as expected for a dataclass."""
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic"
        )
        
        # Should have dataclass fields
        assert hasattr(context, '__dataclass_fields__')
        
        # Fields should be in the expected order
        field_names = list(context.__dataclass_fields__.keys())
        expected_fields = ['message_id', 'topic', 'attempt_count', 'original_timestamp', 'metadata']
        assert field_names == expected_fields
    
    def test_attempt_count_edge_cases(self):
        """Test edge cases for attempt_count."""
        # Zero attempts
        context1 = DeliveryContext(
            message_id="test-msg",
            topic="test-topic",
            attempt_count=0
        )
        assert context1.attempt_count == 0
        
        # Large number of attempts
        context2 = DeliveryContext(
            message_id="test-msg",
            topic="test-topic", 
            attempt_count=1000
        )
        assert context2.attempt_count == 1000
        
        # Negative attempts (unusual but allowed by dataclass)
        context3 = DeliveryContext(
            message_id="test-msg",
            topic="test-topic",
            attempt_count=-1
        )
        assert context3.attempt_count == -1
    
    def test_metadata_mutation(self):
        """Test that metadata can be safely modified after creation."""
        context = DeliveryContext(
            message_id="test-msg",
            topic="test-topic"
        )
        
        # Start empty
        assert context.metadata == {}
        
        # Add data
        context.metadata["key1"] = "value1"
        assert context.metadata["key1"] == "value1"
        
        # Modify data
        context.metadata["key1"] = "new_value"
        assert context.metadata["key1"] == "new_value"
        
        # Add more data
        context.metadata["key2"] = {"nested": "dict"}
        assert len(context.metadata) == 2
        assert context.metadata["key2"]["nested"] == "dict"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])