"""Cezzis OpenTelemetry - Lightweight OpenTelemetry setup for Python services."""

from .otel import (
    get_logger,
    get_propagation_headers,
    initialize_otel,
    shutdown_otel,
)
from .otel_settings import OTelSettings

# Dynamically read version from package metadata
try:
    from importlib.metadata import version

    __version__ = version("cezzis_otel")
except Exception:
    __version__ = "unknown"

__all__ = [
    # Settings
    "OTelSettings",
    # Main functions
    "initialize_otel",
    "shutdown_otel",
    "get_logger",
    "get_propagation_headers",
    # Metadata
    "__version__",
]
