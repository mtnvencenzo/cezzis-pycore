"""Unit tests for otel_settings module."""

import socket
from unittest.mock import patch

import pytest

from cezzis_otel.otel_settings import OTelSettings


class TestOTelSettings:
    """Unit tests for otel_settings module.

    This test class contains tests to verify that the OTelSettings class
    correctly assigns configuration values from the provided parameters.
    """

    def test_assigns_setting_values_correctly(self) -> None:
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
            enable_logging=False,
            enable_tracing=True,
        )
        assert settings.service_name == "test-service"
        assert settings.service_namespace == "test-namespace"
        assert settings.service_version == "1.0.0"
        assert settings.otlp_exporter_endpoint == "http://localhost:4318"
        assert settings.otlp_exporter_auth_header == "Bearer test-token"
        assert settings.environment == "production"
        assert settings.instance_id == "test-instance"
        assert settings.enable_logging is False
        assert settings.enable_tracing is True

    def test_uses_defaults_correctly(self) -> None:
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
        )
        assert settings.service_name == "test-service"
        assert settings.service_namespace == "test-namespace"
        assert settings.service_version == "1.0.0"
        assert settings.otlp_exporter_endpoint == "http://localhost:4318"
        assert settings.otlp_exporter_auth_header == "Bearer test-token"
        assert settings.environment == "production"
        assert settings.instance_id == "test-instance"
        # Test default values for enable_* flags
        assert settings.enable_logging is True
        assert settings.enable_tracing is True

    def test_handles_empty_service_name_with_default(self) -> None:
        settings = OTelSettings(
            service_name="",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
        )
        assert settings.service_name == "unknown"
        assert settings.service_namespace == "test-namespace"
        assert settings.service_version == "1.0.0"

    def test_handles_none_service_namespace_with_default(self) -> None:
        settings = OTelSettings(
            service_name="test-service",
            service_namespace=None,  # type: ignore[arg-type]
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
        )
        assert settings.service_name == "test-service"
        assert settings.service_namespace == "unknown"
        assert settings.service_version == "1.0.0"

    def test_handles_whitespace_service_version_with_default(self) -> None:
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="   ",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
        )
        assert settings.service_name == "test-service"
        assert settings.service_namespace == "test-namespace"
        assert settings.service_version == "unknown"

    def test_handles_empty_environment_with_default(self) -> None:
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="",
            instance_id="test-instance",
        )
        assert settings.service_name == "test-service"
        assert settings.environment == "unknown"
        assert settings.instance_id == "test-instance"

    @patch("socket.gethostname", return_value="test-hostname")
    def test_handles_empty_instance_id_with_hostname_default(self, mock_hostname) -> None:
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="",
        )
        assert settings.service_name == "test-service"
        assert settings.environment == "production"
        assert settings.instance_id == "test-hostname"
        mock_hostname.assert_called_once()

    def test_preserves_otlp_endpoint_and_auth_header(self) -> None:
        endpoint = "https://api.honeycomb.io:443"
        auth_header = "Bearer my-secret-token"

        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint=endpoint,
            otlp_exporter_auth_header=auth_header,
            environment="production",
            instance_id="test-instance",
        )

        assert settings.otlp_exporter_endpoint == endpoint
        assert settings.otlp_exporter_auth_header == auth_header
