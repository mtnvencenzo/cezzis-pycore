"""Unit tests for otel module."""

import logging
import os
from unittest.mock import MagicMock, call, patch

import pytest

from cezzis_otel.otel import (
    _initialize_logging,
    _initialize_tracing,
    get_logger,
    get_propagation_headers,
    initialize_otel,
    log_provider,
    shutdown_otel,
    trace_provider,
)
from cezzis_otel.otel_settings import OTelSettings


class TestInitializeOtel:
    """Test cases for the initialize_otel function."""

    def setup_method(self):
        """Reset global providers before each test."""
        import cezzis_otel.otel

        cezzis_otel.otel.trace_provider = None
        cezzis_otel.otel.log_provider = None

    @patch("cezzis_otel.otel._initialize_logging")
    @patch("cezzis_otel.otel._initialize_tracing")
    @patch("cezzis_otel.otel.Resource")
    @patch("socket.gethostname", return_value="test-hostname")
    def test_initialize_otel_creates_resource_with_correct_attributes(
        self, mock_hostname, mock_resource, mock_init_tracing, mock_init_logging
    ):
        """Test that initialize_otel creates a resource with correct service attributes."""
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
        )

        initialize_otel(settings)

        # Verify Resource is created with correct attributes
        mock_resource.assert_called_once_with(
            attributes={
                "service.name": "test-service",
                "service.namespace": "test-namespace",
                "service.instance.id": "test-hostname",
                "service.version": "1.0.0",
                "deployment.environment": "production",
            }
        )

        # Verify both tracing and logging initialization are called
        mock_init_tracing.assert_called_once()
        mock_init_logging.assert_called_once()

    @patch("cezzis_otel.otel._initialize_logging")
    @patch("cezzis_otel.otel._initialize_tracing")
    @patch("cezzis_otel.otel.Resource")
    def test_initialize_otel_calls_custom_configure_functions(
        self, mock_resource, mock_init_tracing, mock_init_logging
    ):
        """Test that initialize_otel calls custom configure functions when provided."""
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
        )

        mock_configure_tracing = MagicMock()
        mock_configure_logging = MagicMock()

        initialize_otel(settings, mock_configure_tracing, mock_configure_logging)

        # Verify custom configure functions are passed to initialization functions
        args_tracing = mock_init_tracing.call_args[0]
        args_logging = mock_init_logging.call_args[0]

        assert args_tracing[2] == mock_configure_tracing  # Third argument should be configure function
        assert args_logging[2] == mock_configure_logging


class TestInitializeTracing:
    """Test cases for the _initialize_tracing function."""

    def setup_method(self):
        """Reset global providers before each test."""
        import cezzis_otel.otel

        cezzis_otel.otel.trace_provider = None

    @patch("cezzis_otel.otel.trace")
    @patch("cezzis_otel.otel.TraceProvider")
    @patch("cezzis_otel.otel.OTLPSpanExporter")
    @patch("cezzis_otel.otel.BatchSpanProcessor")
    def test_initialize_tracing_with_otlp_endpoint(
        self, mock_batch_processor, mock_otlp_exporter, mock_trace_provider_class, mock_trace
    ):
        """Test tracing initialization with OTLP endpoint."""
        mock_resource = MagicMock()
        mock_trace_provider_instance = MagicMock()
        mock_trace_provider_class.return_value = mock_trace_provider_instance

        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
            enable_tracing=True,
        )

        _initialize_tracing(mock_resource, settings)

        # Verify TraceProvider creation
        mock_trace_provider_class.assert_called_once_with(resource=mock_resource)
        mock_trace.set_tracer_provider.assert_called_once_with(mock_trace_provider_instance)

        # Verify OTLP exporter setup
        mock_otlp_exporter.assert_called_once_with(
            endpoint="http://localhost:4318/v1/traces", headers={"authorization": "Bearer test-token"}
        )

        # Verify span processor setup
        mock_batch_processor.assert_called_once_with(mock_otlp_exporter.return_value)
        mock_trace_provider_instance.add_span_processor.assert_called_once_with(mock_batch_processor.return_value)

    def test_initialize_tracing_disabled(self):
        """Test that tracing initialization is skipped when disabled."""
        mock_resource = MagicMock()
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
            enable_tracing=False,
        )

        with patch("cezzis_otel.otel.TraceProvider") as mock_trace_provider:
            _initialize_tracing(mock_resource, settings)
            mock_trace_provider.assert_not_called()

    @patch("cezzis_otel.otel.trace")
    @patch("cezzis_otel.otel.TraceProvider")
    def test_initialize_tracing_without_otlp_endpoint(self, mock_trace_provider_class, mock_trace):
        """Test tracing initialization without OTLP endpoint."""
        mock_resource = MagicMock()
        mock_trace_provider_instance = MagicMock()
        mock_trace_provider_class.return_value = mock_trace_provider_instance

        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="",  # Empty endpoint
            otlp_exporter_auth_header="",
            environment="production",
            instance_id="test-instance",
            enable_tracing=True,
        )

        with patch("cezzis_otel.otel.OTLPSpanExporter") as mock_otlp_exporter:
            _initialize_tracing(mock_resource, settings)

            # Verify TraceProvider is still created
            mock_trace_provider_class.assert_called_once_with(resource=mock_resource)
            mock_trace.set_tracer_provider.assert_called_once_with(mock_trace_provider_instance)

            # Verify OTLP exporter is NOT created when endpoint is empty
            mock_otlp_exporter.assert_not_called()

    @patch("cezzis_otel.otel.trace")
    @patch("cezzis_otel.otel.TraceProvider")
    def test_initialize_tracing_calls_configure_callback(self, mock_trace_provider_class, mock_trace):
        """Test that tracing initialization calls custom configure callback."""
        mock_resource = MagicMock()
        mock_trace_provider_instance = MagicMock()
        mock_trace_provider_class.return_value = mock_trace_provider_instance
        mock_configure = MagicMock()

        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="",
            otlp_exporter_auth_header="",
            environment="production",
            instance_id="test-instance",
            enable_tracing=True,
        )

        _initialize_tracing(mock_resource, settings, mock_configure)

        mock_configure.assert_called_once_with(mock_trace_provider_instance)


class TestInitializeLogging:
    """Test cases for the _initialize_logging function."""

    def setup_method(self):
        """Reset global providers and clear handlers before each test."""
        import cezzis_otel.otel

        cezzis_otel.otel.log_provider = None
        # Clear existing handlers
        logging.getLogger().handlers.clear()

    @patch("cezzis_otel.otel.set_logger_provider")
    @patch("cezzis_otel.otel.LoggerProvider")
    @patch("cezzis_otel.otel.OTLPLogExporter")
    @patch("cezzis_otel.otel.BatchLogRecordProcessor")
    @patch("cezzis_otel.otel.LoggingHandler")
    def test_initialize_logging_with_otlp_endpoint(
        self,
        mock_logging_handler,
        mock_batch_processor,
        mock_otlp_exporter,
        mock_logger_provider_class,
        mock_set_logger_provider,
    ):
        """Test logging initialization with OTLP endpoint."""
        mock_resource = MagicMock()
        mock_logger_provider_instance = MagicMock()
        mock_logger_provider_class.return_value = mock_logger_provider_instance

        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
            enable_logging=True,
        )

        _initialize_logging(mock_resource, settings)

        # Verify LoggerProvider creation
        mock_logger_provider_class.assert_called_once_with(resource=mock_resource, shutdown_on_exit=True)
        mock_set_logger_provider.assert_called_once_with(mock_logger_provider_instance)

        # Verify OTLP exporter setup
        mock_otlp_exporter.assert_called_once_with(
            endpoint="http://localhost:4318/v1/logs", headers={"authorization": "Bearer test-token"}
        )

        # Verify log processor setup
        mock_batch_processor.assert_called_once_with(mock_otlp_exporter.return_value)
        mock_logger_provider_instance.add_log_record_processor.assert_called_once_with(
            mock_batch_processor.return_value
        )

        # Verify logging handler setup
        mock_logging_handler.assert_called_once_with(
            level=logging.NOTSET, logger_provider=mock_logger_provider_instance
        )

    @patch("logging.basicConfig")
    def test_initialize_logging_disabled(self, mock_basic_config):
        """Test that logging falls back to basicConfig when disabled."""
        mock_resource = MagicMock()
        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="http://localhost:4318",
            otlp_exporter_auth_header="Bearer test-token",
            environment="production",
            instance_id="test-instance",
            enable_logging=False,
        )

        _initialize_logging(mock_resource, settings)

        mock_basic_config.assert_called_once_with(level=logging.INFO)

    @patch.dict(os.environ, {"ENV": "local"})
    @patch("cezzis_otel.otel.set_logger_provider")
    @patch("cezzis_otel.otel.LoggerProvider")
    @patch("cezzis_otel.otel.LoggingHandler")
    def test_initialize_logging_adds_console_handler_for_local_env(
        self, mock_logging_handler, mock_logger_provider_class, mock_set_logger_provider
    ):
        """Test that console handler is added in local environment."""
        mock_resource = MagicMock()
        mock_logger_provider_instance = MagicMock()
        mock_logger_provider_class.return_value = mock_logger_provider_instance

        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="",
            otlp_exporter_auth_header="",
            environment="production",
            instance_id="test-instance",
            enable_logging=True,
        )

        with patch("logging.StreamHandler") as mock_stream_handler:
            with patch("logging.Formatter") as mock_formatter:
                _initialize_logging(mock_resource, settings)

                # Verify console handler creation
                mock_stream_handler.assert_called_once()
                mock_stream_handler.return_value.setLevel.assert_called_once_with(logging.INFO)

                # Verify formatter creation
                mock_formatter.assert_called_once_with(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )

    @patch("cezzis_otel.otel.set_logger_provider")
    @patch("cezzis_otel.otel.LoggerProvider")
    def test_initialize_logging_calls_configure_callback(self, mock_logger_provider_class, mock_set_logger_provider):
        """Test that logging initialization calls custom configure callback."""
        mock_resource = MagicMock()
        mock_logger_provider_instance = MagicMock()
        mock_logger_provider_class.return_value = mock_logger_provider_instance
        mock_configure = MagicMock()

        settings = OTelSettings(
            service_name="test-service",
            service_namespace="test-namespace",
            service_version="1.0.0",
            otlp_exporter_endpoint="",
            otlp_exporter_auth_header="",
            environment="production",
            instance_id="test-instance",
            enable_logging=True,
        )

        _initialize_logging(mock_resource, settings, mock_configure)

        mock_configure.assert_called_once_with(mock_logger_provider_instance)


class TestShutdownOtel:
    """Test cases for the shutdown_otel function."""

    @patch("logging.info")  # Mock the logging.info calls in shutdown_otel
    @patch("logging.getLogger")
    def test_shutdown_otel_with_providers(self, mock_get_logger, mock_logging_info):
        """Test shutdown when both providers are initialized."""
        import cezzis_otel.otel

        # Set up mock providers
        mock_trace_provider = MagicMock()
        mock_log_provider = MagicMock()
        cezzis_otel.otel.trace_provider = mock_trace_provider
        cezzis_otel.otel.log_provider = mock_log_provider

        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        shutdown_otel()

        # Verify trace provider shutdown
        mock_trace_provider.force_flush.assert_called_once()
        mock_trace_provider.shutdown.assert_called_once()

        # Verify log provider shutdown
        mock_log_provider.force_flush.assert_called_once()
        mock_log_provider.shutdown.assert_called_once()

        # Verify logging
        mock_logger.info.assert_called_once_with("Shutting down opentelemetry providers")

        # Verify the logging.info calls for trace and log provider shutdown
        expected_calls = [
            call("Flushing and shutting down trace provider"),
            call("Flushing and shutting down log provider"),
        ]
        mock_logging_info.assert_has_calls(expected_calls)

    @patch("cezzis_otel.otel.inject", lambda h: h.update({"trace-id": "abc123", "user": "alice"}))
    def test_get_propagation_headers_basic(mocker):
        # Mock inject to populate headers
        result = get_propagation_headers()
        assert isinstance(result, dict)
        assert result["trace-id"] == b"abc123"
        assert result["user"] == b"alice"

    @patch("cezzis_otel.otel.inject", lambda h: h.update({"trace-id": "abc123"}))
    def test_get_propagation_headers_with_extra(mocker):
        extra = {"custom": b"value", "trace-id": b"override"}
        result = get_propagation_headers(extra)
        # extra should override trace-id
        assert result["trace-id"] == b"override"
        assert result["custom"] == b"value"

    @patch("cezzis_otel.otel.inject", lambda h: h.update({"trace-id": 123, "user": b"bob"}))
    def test_get_propagation_headers_non_str_values(mocker):
        result = get_propagation_headers()
        # int should not be encoded, bytes should pass through
        assert result["trace-id"] == 123
        assert result["user"] == b"bob"

    @patch("logging.getLogger")
    def test_shutdown_otel_with_no_providers(self, mock_get_logger):
        """Test shutdown when no providers are initialized."""
        import cezzis_otel.otel

        # Set providers to None
        cezzis_otel.otel.trace_provider = None
        cezzis_otel.otel.log_provider = None

        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        # Should not raise any exceptions
        shutdown_otel()

        mock_logger.info.assert_called_once_with("Shutting down opentelemetry providers")


class TestGetLogger:
    """Test cases for the get_logger function."""

    @patch("logging.getLogger")
    def test_get_logger_returns_configured_logger(self, mock_get_logger):
        """Test that get_logger returns a properly configured logger."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        result = get_logger("test-logger", logging.DEBUG)

        mock_get_logger.assert_called_once_with("test-logger")
        mock_logger.setLevel.assert_called_once_with(logging.DEBUG)
        assert result == mock_logger

    @patch("logging.getLogger")
    def test_get_logger_uses_default_level(self, mock_get_logger):
        """Test that get_logger uses INFO level by default."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        result = get_logger("test-logger")

        mock_get_logger.assert_called_once_with("test-logger")
        mock_logger.setLevel.assert_called_once_with(logging.INFO)
        assert result == mock_logger
