# ----------------------------------------------------------------------------------------------
# https://github.com/open-telemetry/opentelemetry-python/blob/main/docs/examples/logs/example.py
# ----------------------------------------------------------------------------------------------
import logging
import socket
from typing import Callable, Optional

from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import inject
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Attributes, Resource
from opentelemetry.sdk.trace import TracerProvider as TraceProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from cezzis_otel.otel_settings import OTelSettings

# Define global variables for trace and log providers
trace_provider: TraceProvider | None = None
log_provider: LoggerProvider | None = None


def initialize_otel(
    settings: OTelSettings,
    configure_tracing: Optional[Callable[[TraceProvider], None]] = None,
    configure_logging: Optional[Callable[[LoggerProvider], None]] = None,
    resource_attributes: Optional[Attributes] = None,
) -> None:
    """Initialize OpenTelemetry tracing and logging.

    Args:
        settings (OTelSettings): The OpenTelemetry settings for configuration.
        configure_tracing (Optional[Callable[[TraceProvider], None]]): A callback function to configure the trace provider.
        configure_logging (Optional[Callable[[LoggerProvider], None]]): A callback function to configure the log provider.
        resource_attributes (Optional[Attributes]): Additional resource attributes to include.

    Returns:
        None
    """

    print("Initializing OpenTelemetry")

    resource = Resource(
        attributes={
            "service.name": settings.service_name,
            "service.namespace": settings.service_namespace,
            "service.instance.id": socket.gethostname(),
            "service.version": settings.service_version,
            "deployment.environment": settings.environment,
            **(resource_attributes or {}),
        }
    )

    _initialize_tracing(resource, settings, configure_tracing)
    _initialize_logging(resource, settings, configure_logging)


def _initialize_tracing(
    resource: Resource, settings: OTelSettings, configure: Optional[Callable[[TraceProvider], None]] = None
) -> None:
    """Initialize only OpenTelemetry tracing.

    Args:
        resource (Resource): The OpenTelemetry resource describing the service.
        settings (OTelSettings): The OpenTelemetry settings for configuration.
        configure (Optional[Callable[[TraceProvider], None]]): A callback function to configure the trace provider.

    Returns:
        None
    """
    if not settings.enable_tracing:
        return

    global trace_provider

    # Setup tracing with the otlp exporter to an otel collector
    trace_provider = TraceProvider(resource=resource)
    trace.set_tracer_provider(trace_provider)

    if settings.otlp_exporter_endpoint:
        otlp_exporter = OTLPSpanExporter(
            endpoint=f"{settings.otlp_exporter_endpoint}/v1/traces",
            headers={"authorization": settings.otlp_exporter_auth_header},
            certificate_file=settings.certificate_file,
            client_certificate_file=settings.client_certificate_file,
            client_key_file=settings.client_key_file,
        )

        span_processor = BatchSpanProcessor(otlp_exporter)
        trace_provider.add_span_processor(span_processor)

    if configure:
        configure(trace_provider)


def _initialize_logging(
    resource: Resource, settings: OTelSettings, configure: Optional[Callable[[LoggerProvider], None]] = None
) -> None:
    """Initialize only OpenTelemetry logging.

    Args:
        resource (Resource): The OpenTelemetry resource describing the service.
        settings (OTelSettings): The OpenTelemetry settings for configuration.
        configure (Optional[Callable[[LoggerProvider], None]]): A callback function to configure the log provider.

    Returns:
        None
    """
    if not settings.enable_logging:
        logging.basicConfig(level=logging.INFO)
        return

    global log_provider

    log_provider = LoggerProvider(resource=resource, shutdown_on_exit=True)
    set_logger_provider(log_provider)

    # Add OTLP exporter for remote telemetry
    if settings.otlp_exporter_endpoint:
        otlp_log_exporter = OTLPLogExporter(
            endpoint=f"{settings.otlp_exporter_endpoint}/v1/logs",
            headers={"authorization": settings.otlp_exporter_auth_header},
            certificate_file=settings.certificate_file,
            client_certificate_file=settings.client_certificate_file,
            client_key_file=settings.client_key_file,
        )
        otlp_log_processor = BatchLogRecordProcessor(otlp_log_exporter)
        log_provider.add_log_record_processor(otlp_log_processor)

    # Add OpenTelemetry handler for OTLP export
    otel_handler = LoggingHandler(level=logging.NOTSET, logger_provider=log_provider)

    # Set the root logger level to NOTSET to ensure all messages are captured
    logging.getLogger().setLevel(logging.NOTSET)

    # Attach both handlers to root logger
    logging.getLogger().addHandler(otel_handler)

    # Add simple console handler for readable local output
    if settings.enable_console_logging:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)

        logging.getLogger().addHandler(console_handler)

    if configure:
        configure(log_provider)


def shutdown_otel() -> None:
    """Shutdown the application logger and close OpenTelemetry if initialized."""
    global trace_provider
    global log_provider

    logger = logging.getLogger(__name__)
    logger.info("Shutting down opentelemetry providers")

    if trace_provider:
        logging.info("Flushing and shutting down trace provider")
        trace_provider.force_flush()
        trace_provider.shutdown()

    if log_provider:
        logging.info("Flushing and shutting down log provider")
        log_provider.force_flush()
        log_provider.shutdown()


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get an OpenTelemetry-instrumented logger.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: The OpenTelemetry-instrumented logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger


def get_propagation_headers(extra: dict[str, str | bytes] = {}) -> dict[str, str | bytes]:
    """Get the current propagation headers for context propagation.

    Args:
        extra (dict[str, bytes]): Additional headers to include. Values must be bytes.

    Returns:
        dict[str, bytes]: A dictionary of propagation headers (all values are bytes).
    """

    # Create headers dict for trace propagation
    headers = {}
    inject(headers)

    # Convert string values to bytes for Kafka headers
    injected_headers = {
        key: value.encode("utf-8") if isinstance(value, str) else value for key, value in headers.items()
    }

    injected_headers.update(extra)

    return injected_headers
