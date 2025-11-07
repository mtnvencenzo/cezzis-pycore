# Cezzis OpenTelemetry

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/cezzis-otel.svg)](https://pypi.org/project/cezzis-otel/)

A lightweight, production-ready Python library for OpenTelemetry observability. Simplifies tracing and logging setup with automatic OTLP exporter integration and structured service instrumentation.

## Installation

Install `cezzis-otel` from PyPI:

```bash
pip install cezzis-otel
```

Or using Poetry:

```bash
poetry add cezzis-otel
```

## Requirements

- Python 3.12 or higher
- OpenTelemetry Collector (optional, for remote telemetry)

## Source Code

Find the source code, contribute, or report issues on GitHub:

**Repository:** [https://github.com/mtnvencenzo/cezzis-pycore](https://github.com/mtnvencenzo/cezzis-pycore)

## Key Features

- **Simple OpenTelemetry Setup** - One-line initialization for tracing and logging
- **OTLP Integration** - Built-in OTLP exporter configuration for popular observability platforms
- **Service Resource Management** - Automatic service metadata and resource attribution
- **Flexible Configuration** - Comprehensive settings for all OpenTelemetry options
- **Production Ready** - Built-in error handling and graceful shutdown capabilities
- **Type-Safe** - Full type hints for better IDE support and code quality

## Quick Start Guide

### Basic Example: Simple Service Setup

Here's a minimal example to get started with OpenTelemetry in your Python service:

```python
from cezzis_otel import OTelSettings, initialize_otel, get_logger, shutdown_otel
import logging
import time

def main():
    # Configure OpenTelemetry settings
    settings = OTelSettings(
        service_name="my-python-service",
        service_namespace="production",
        service_version="1.0.0",
        otlp_exporter_endpoint="https://api.honeycomb.io",
        otlp_exporter_auth_header="Bearer your-api-key",
        environment="production",
        instance_id="web-server-01"
    )
    
    # Initialize OpenTelemetry with one line
    initialize_otel(settings)
    
    # Get an instrumented logger
    logger = get_logger(__name__)
    
    try:
        logger.info("Service starting up")
        
        # Your application logic here
        for i in range(5):
            logger.info(f"Processing item {i}")
            time.sleep(1)
            
        logger.info("Service completed successfully")
        
    except Exception as e:
        logger.error(f"Service failed: {e}")
        raise
    finally:
        # Clean shutdown
        shutdown_otel()

if __name__ == "__main__":
    main()
```

### Example: Local Development Setup

For local development with an OpenTelemetry Collector:

```python
from cezzis_otel import OTelSettings, initialize_otel, get_logger, shutdown_otel

# Configure for local development
settings = OTelSettings(
    service_name="local-dev-service", 
    service_namespace="development",
    service_version="0.1.0",
    otlp_exporter_endpoint="http://localhost:4318",  # Local collector
    otlp_exporter_auth_header="",  # No auth for local
    environment="local",
    instance_id="dev-machine"
)

# Initialize and use
initialize_otel(settings)
logger = get_logger(__name__)

logger.info("Local development setup complete")
logger.debug("This will include trace context automatically")

# Don't forget cleanup
shutdown_otel()
```

## API Reference

### `OTelSettings`

Configuration class for OpenTelemetry setup.

**Parameters:**
- `service_name` (str): Name of your service (required)
- `service_namespace` (str): Service namespace/team (required)  
- `service_version` (str): Version of your service (required)
- `otlp_exporter_endpoint` (str): OTLP collector endpoint URL (required)
- `otlp_exporter_auth_header` (str): Authorization header for OTLP exporter (required)
- `environment` (str): Environment name (e.g., "production", "staging") (required)
- `instance_id` (str): Unique instance identifier (required)
- `enable_logging` (bool): Enable OpenTelemetry logging (default: True)
- `enable_tracing` (bool): Enable OpenTelemetry tracing (default: True)

**Example:**
```python
settings = OTelSettings(
    service_name="user-api",
    service_namespace="backend-services", 
    service_version="2.1.0",
    otlp_exporter_endpoint="https://api.honeycomb.io",
    otlp_exporter_auth_header="Bearer your-api-key",
    environment="production",
    instance_id="api-server-03"
)
```

### `initialize_otel(settings, configure_tracing=None, configure_logging=None)`

Initialize OpenTelemetry tracing and logging with the provided settings.

**Parameters:**
- `settings` (OTelSettings): Configuration object for OpenTelemetry setup
- `configure_tracing` (Optional[Callable]): Optional callback to customize trace provider
- `configure_logging` (Optional[Callable]): Optional callback to customize log provider

**Example:**
```python
from cezzis_otel import initialize_otel, OTelSettings

settings = OTelSettings(...)
initialize_otel(settings)
```

### `get_logger(name, level=logging.INFO)`

Get an OpenTelemetry-instrumented logger instance.

**Parameters:**
- `name` (str): Logger name (typically `__name__`)
- `level` (int): Logging level (default: logging.INFO)

**Returns:**
- `logging.Logger`: Configured logger with OpenTelemetry integration

**Example:**
```python
from cezzis_otel import get_logger
import logging

logger = get_logger(__name__, level=logging.DEBUG)
logger.info("This message includes trace context automatically")
```

### `shutdown_otel()`

Gracefully shutdown OpenTelemetry providers and flush any pending telemetry data.

**Example:**
```python
from cezzis_otel import shutdown_otel

# At application shutdown
shutdown_otel()
```

### 2. Structured Logging

Use structured logging with contextual information:

```python
logger = get_logger(__name__)

def process_user_login(user_id, ip_address):
    logger.info(
        "User login attempt",
        extra={
            "user_id": user_id,
            "ip_address": ip_address,
            "action": "login"
        }
    )
```

### 3. Error Handling

Always log errors with proper context:

```python
def risky_operation(data):
    try:
        result = process_data(data)
        logger.info("Operation completed successfully")
        return result
    except Exception as e:
        logger.error(
            "Operation failed", 
            extra={"data_size": len(data), "error_type": type(e).__name__},
            exc_info=True
        )
        raise
```

### 4. Resource Cleanup

Always shut down OpenTelemetry properly:

```python
import signal
import sys
from cezzis_otel import shutdown_otel

def signal_handler(sig, frame):
    print("Shutting down gracefully...")
    shutdown_otel()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
```

## Troubleshooting

### Telemetry Not Appearing

1. Verify OTLP endpoint is reachable: `curl -v https://your-endpoint/v1/traces`
2. Check authentication headers are correct
3. Ensure `initialize_otel()` is called before logging
4. Verify the OpenTelemetry Collector is running and configured

### Missing Trace Context

- Ensure you're using `get_logger()` from cezzis_otel
- Check that `initialize_otel()` completed successfully
- Verify tracing is enabled: `enable_tracing=True` in settings

### Performance Impact

- Use appropriate log levels (avoid DEBUG in production)
- Monitor OTLP exporter endpoint latency
- Consider batch export intervals for high-volume applications
- Set reasonable resource limits for trace/log providers

## Contributing

We welcome contributions! Visit the [GitHub repository](https://github.com/mtnvencenzo/cezzis-pycore) to:

- Report bugs or request features via [Issues](https://github.com/mtnvencenzo/cezzis-pycore/issues)
- Submit pull requests with improvements
- Read the [Contributing Guide](https://github.com/mtnvencenzo/cezzis-pycore/blob/main/.github/CONTRIBUTING.md)

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/mtnvencenzo/cezzis-pycore/blob/main/LICENSE) file for details.

## Support

- **Issues:** [GitHub Issues](https://github.com/mtnvencenzo/cezzis-pycore/issues)
- **Discussions:** [GitHub Discussions](https://github.com/mtnvencenzo/cezzis-pycore/discussions)

## Acknowledgments

Built with [OpenTelemetry Python](https://github.com/open-telemetry/opentelemetry-python), the official Python implementation of OpenTelemetry.

---

**Happy observing! �**
