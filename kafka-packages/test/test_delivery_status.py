"""
Tests for DeliveryStatus enum class.
"""

import pytest
from enum import Enum
from cezzis_kafka.delivery_status import DeliveryStatus


class TestDeliveryStatus:
    """Test suite for DeliveryStatus enum."""
    
    def test_enum_values(self):
        """Test that all expected enum values exist with correct string values."""
        assert DeliveryStatus.SUCCESS.value == "success"
        assert DeliveryStatus.RETRIABLE_ERROR.value == "retriable_error"
        assert DeliveryStatus.FATAL_ERROR.value == "fatal_error"
        assert DeliveryStatus.TIMEOUT.value == "timeout"
    
    def test_enum_members_count(self):
        """Test that the enum has the expected number of members."""
        expected_count = 4
        actual_count = len(DeliveryStatus)
        assert actual_count == expected_count
    
    def test_enum_membership(self):
        """Test that each status is a member of the enum."""
        assert DeliveryStatus.SUCCESS in DeliveryStatus
        assert DeliveryStatus.RETRIABLE_ERROR in DeliveryStatus
        assert DeliveryStatus.FATAL_ERROR in DeliveryStatus
        assert DeliveryStatus.TIMEOUT in DeliveryStatus
    
    def test_enum_inheritance(self):
        """Test that DeliveryStatus inherits from Enum."""
        assert issubclass(DeliveryStatus, Enum)
        assert isinstance(DeliveryStatus.SUCCESS, DeliveryStatus)
        assert isinstance(DeliveryStatus.SUCCESS, Enum)
    
    def test_enum_names(self):
        """Test that enum members have correct names."""
        assert DeliveryStatus.SUCCESS.name == "SUCCESS"
        assert DeliveryStatus.RETRIABLE_ERROR.name == "RETRIABLE_ERROR"
        assert DeliveryStatus.FATAL_ERROR.name == "FATAL_ERROR"
        assert DeliveryStatus.TIMEOUT.name == "TIMEOUT"
    
    def test_enum_string_representation(self):
        """Test string representation of enum members."""
        assert str(DeliveryStatus.SUCCESS) == "DeliveryStatus.SUCCESS"
        assert str(DeliveryStatus.RETRIABLE_ERROR) == "DeliveryStatus.RETRIABLE_ERROR"
        assert str(DeliveryStatus.FATAL_ERROR) == "DeliveryStatus.FATAL_ERROR"
        assert str(DeliveryStatus.TIMEOUT) == "DeliveryStatus.TIMEOUT"
    
    def test_enum_repr(self):
        """Test repr representation of enum members."""
        assert repr(DeliveryStatus.SUCCESS) == "<DeliveryStatus.SUCCESS: 'success'>"
        assert repr(DeliveryStatus.RETRIABLE_ERROR) == "<DeliveryStatus.RETRIABLE_ERROR: 'retriable_error'>"
        assert repr(DeliveryStatus.FATAL_ERROR) == "<DeliveryStatus.FATAL_ERROR: 'fatal_error'>"
        assert repr(DeliveryStatus.TIMEOUT) == "<DeliveryStatus.TIMEOUT: 'timeout'>"
    
    def test_enum_equality(self):
        """Test equality comparison between enum members."""
        # Same members should be equal
        assert DeliveryStatus.SUCCESS == DeliveryStatus.SUCCESS
        assert DeliveryStatus.RETRIABLE_ERROR == DeliveryStatus.RETRIABLE_ERROR
        
        # Different members should not be equal
        assert DeliveryStatus.SUCCESS != DeliveryStatus.FATAL_ERROR
        assert DeliveryStatus.RETRIABLE_ERROR != DeliveryStatus.TIMEOUT
    
    def test_enum_identity(self):
        """Test identity comparison between enum members."""
        # Same members should have same identity
        assert DeliveryStatus.SUCCESS is DeliveryStatus.SUCCESS
        assert DeliveryStatus.RETRIABLE_ERROR is DeliveryStatus.RETRIABLE_ERROR
        
        # Different members should have different identity
        assert DeliveryStatus.SUCCESS is not DeliveryStatus.FATAL_ERROR
        assert DeliveryStatus.RETRIABLE_ERROR is not DeliveryStatus.TIMEOUT
    
    def test_enum_iteration(self):
        """Test that we can iterate over enum members."""
        statuses = list(DeliveryStatus)
        expected_statuses = [
            DeliveryStatus.SUCCESS,
            DeliveryStatus.RETRIABLE_ERROR,
            DeliveryStatus.FATAL_ERROR,
            DeliveryStatus.TIMEOUT
        ]
        
        assert statuses == expected_statuses
        assert len(statuses) == 4
    
    def test_enum_lookup_by_name(self):
        """Test looking up enum members by name."""
        assert DeliveryStatus['SUCCESS'] == DeliveryStatus.SUCCESS
        assert DeliveryStatus['RETRIABLE_ERROR'] == DeliveryStatus.RETRIABLE_ERROR
        assert DeliveryStatus['FATAL_ERROR'] == DeliveryStatus.FATAL_ERROR
        assert DeliveryStatus['TIMEOUT'] == DeliveryStatus.TIMEOUT
    
    def test_enum_lookup_by_value(self):
        """Test looking up enum members by value."""
        assert DeliveryStatus("success") == DeliveryStatus.SUCCESS
        assert DeliveryStatus("retriable_error") == DeliveryStatus.RETRIABLE_ERROR
        assert DeliveryStatus("fatal_error") == DeliveryStatus.FATAL_ERROR
        assert DeliveryStatus("timeout") == DeliveryStatus.TIMEOUT
    
    def test_enum_invalid_name_lookup(self):
        """Test that invalid name lookup raises KeyError."""
        with pytest.raises(KeyError):
            DeliveryStatus['INVALID_STATUS']
    
    def test_enum_invalid_value_lookup(self):
        """Test that invalid value lookup raises ValueError."""
        with pytest.raises(ValueError):
            DeliveryStatus("invalid_status")
    
    def test_enum_hashable(self):
        """Test that enum members are hashable and can be used in sets/dicts."""
        # Test in set
        status_set = {
            DeliveryStatus.SUCCESS,
            DeliveryStatus.RETRIABLE_ERROR,
            DeliveryStatus.FATAL_ERROR,
            DeliveryStatus.TIMEOUT,
            DeliveryStatus.SUCCESS  # Duplicate
        }
        assert len(status_set) == 4  # No duplicates
        
        # Test as dict keys
        status_dict = {
            DeliveryStatus.SUCCESS: "Message delivered successfully",
            DeliveryStatus.RETRIABLE_ERROR: "Temporary failure, will retry",
            DeliveryStatus.FATAL_ERROR: "Permanent failure",
            DeliveryStatus.TIMEOUT: "Delivery timed out"
        }
        
        assert len(status_dict) == 4
        assert status_dict[DeliveryStatus.SUCCESS] == "Message delivered successfully"
    
    def test_enum_comparison_with_strings(self):
        """Test that enum values can be compared with their string values."""
        assert DeliveryStatus.SUCCESS.value == "success"
        assert DeliveryStatus.RETRIABLE_ERROR.value == "retriable_error"
        
        # Direct enum comparison with strings should not be equal
        assert DeliveryStatus.SUCCESS != "success"
        assert DeliveryStatus.RETRIABLE_ERROR != "retriable_error"
    
    def test_enum_boolean_context(self):
        """Test that enum members are truthy in boolean context."""
        assert bool(DeliveryStatus.SUCCESS)
        assert bool(DeliveryStatus.RETRIABLE_ERROR)
        assert bool(DeliveryStatus.FATAL_ERROR)
        assert bool(DeliveryStatus.TIMEOUT)
    
    @pytest.mark.parametrize("status,expected_value", [
        (DeliveryStatus.SUCCESS, "success"),
        (DeliveryStatus.RETRIABLE_ERROR, "retriable_error"),
        (DeliveryStatus.FATAL_ERROR, "fatal_error"),
        (DeliveryStatus.TIMEOUT, "timeout"),
    ])
    def test_enum_value_mapping(self, status, expected_value):
        """Test parametrized value mapping for all enum members."""
        assert status.value == expected_value
    
    @pytest.mark.parametrize("status_name,expected_status", [
        ("SUCCESS", DeliveryStatus.SUCCESS),
        ("RETRIABLE_ERROR", DeliveryStatus.RETRIABLE_ERROR),
        ("FATAL_ERROR", DeliveryStatus.FATAL_ERROR),
        ("TIMEOUT", DeliveryStatus.TIMEOUT),
    ])
    def test_enum_name_mapping(self, status_name, expected_status):
        """Test parametrized name mapping for all enum members."""
        assert DeliveryStatus[status_name] == expected_status
        assert expected_status.name == status_name
    
    def test_enum_contains_status_logic(self):
        """Test logical grouping of statuses for business logic."""
        # Success statuses
        successful_statuses = {DeliveryStatus.SUCCESS}
        assert DeliveryStatus.SUCCESS in successful_statuses
        
        # Error statuses (retriable and fatal)
        error_statuses = {DeliveryStatus.RETRIABLE_ERROR, DeliveryStatus.FATAL_ERROR}
        assert DeliveryStatus.RETRIABLE_ERROR in error_statuses
        assert DeliveryStatus.FATAL_ERROR in error_statuses
        assert DeliveryStatus.SUCCESS not in error_statuses
        
        # Terminal statuses (won't be retried)
        terminal_statuses = {DeliveryStatus.SUCCESS, DeliveryStatus.FATAL_ERROR}
        assert DeliveryStatus.SUCCESS in terminal_statuses
        assert DeliveryStatus.FATAL_ERROR in terminal_statuses
        assert DeliveryStatus.RETRIABLE_ERROR not in terminal_statuses
    
    def test_enum_serialization_compatibility(self):
        """Test that enum values work well for JSON serialization."""
        # Test that values are JSON-serializable strings
        import json
        
        for status in DeliveryStatus:
            # Should be able to serialize the value
            serialized = json.dumps(status.value)
            deserialized = json.loads(serialized)
            assert deserialized == status.value
            assert isinstance(deserialized, str)
    
    def test_enum_case_sensitivity(self):
        """Test that enum lookups are case-sensitive."""
        # Name lookups are case-sensitive
        with pytest.raises(KeyError):
            DeliveryStatus['success']  # lowercase
        
        with pytest.raises(KeyError):
            DeliveryStatus['Success']  # mixed case
        
        # Value lookups are case-sensitive
        with pytest.raises(ValueError):
            DeliveryStatus('SUCCESS')  # uppercase
        
        with pytest.raises(ValueError):
            DeliveryStatus('Success')  # mixed case
    
    def test_enum_immutability(self):
        """Test that enum members are immutable."""
        original_value = DeliveryStatus.SUCCESS.value
        
        # Cannot change the value
        with pytest.raises(AttributeError):
            DeliveryStatus.SUCCESS.value = "modified"  # type: ignore
        
        # Value should remain unchanged
        assert DeliveryStatus.SUCCESS.value == original_value


if __name__ == "__main__":
    pytest.main([__file__, "-v"])