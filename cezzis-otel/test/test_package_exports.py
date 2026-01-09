"""Test that demonstrates the public API of cezzis_otel package."""

import pytest

from cezzis_otel import OTelSettings, get_logger, initialize_otel, shutdown_otel


def test_package_exports():
    """Test that all expected exports are available from the package."""
    # Test that we can import the main classes and functions
    assert OTelSettings is not None
    assert initialize_otel is not None
    assert shutdown_otel is not None
    assert get_logger is not None


def test_package_metadata():
    """Test that package metadata is accessible."""
    from cezzis_otel import __version__

    assert __version__ == "0.0.1"


def test_package_usage_example():
    """Test that the package can be used as shown in documentation."""
    # This is the basic usage pattern from our README
    settings = OTelSettings(
        service_name="test-service",
        service_namespace="test",
        service_version="1.0.0",
        otlp_exporter_endpoint="http://localhost:4318",
        otlp_exporter_auth_header="",
        environment="test",
        instance_id="test-instance",
    )

    # Verify the settings object was created correctly
    assert settings.service_name == "test-service"
    assert settings.service_namespace == "test"
    assert settings.service_version == "1.0.0"

    # Verify we can get a logger without initializing (for testing)
    logger = get_logger(__name__)
    assert logger is not None
    assert logger.name == __name__
