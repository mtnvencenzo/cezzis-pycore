# � Cezzis OpenTelemetry

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A lightweight, production-ready Python library for working with OpenTelemetry. Simplifies observability setup with automatic tracing, logging, and OTLP exporter integration for modern distributed systems.

## ✨ Features

- � **Easy OpenTelemetry Setup** - Simple, one-line initialization for tracing and logging
- 📊 **OTLP Integration** - Built-in support for OTLP exporters and collectors
- 🏗️ **Flexible Configuration** - Comprehensive settings class for all OpenTelemetry options
- � **Structured Logging** - Rich, contextual logging with OpenTelemetry integration  
- 🌐 **Service Resource Management** - Automatic service metadata and resource attribution
- 🛡️ **Production Ready** - Built-in error handling and graceful shutdown capabilities

## 📦 Installation

### Using Poetry (Recommended)

```bash
poetry add cezzis-otel
```

### Using pip

```bash
pip install cezzis-otel
```

## 🚀 Quick Start

### 1. Configure OpenTelemetry Settings

Create your OpenTelemetry configuration with the `OTelSettings` class:

```python
from cezzis_otel import OTelSettings

# Configure OpenTelemetry settings
settings = OTelSettings(
    service_name="my-awesome-service",
    service_namespace="production",
    service_version="1.0.0",
    otlp_exporter_endpoint="https://api.honeycomb.io",
    otlp_exporter_auth_header="Bearer your-api-key",
    environment="production",
    instance_id="instance-001",
    enable_logging=True,
    enable_tracing=True
)
```

### 2. Initialize OpenTelemetry

Initialize tracing and logging with a single function call:

```python
from cezzis_otel import initialize_otel

# Initialize OpenTelemetry with your settings
initialize_otel(settings)

# Your application code here - tracing and logging are now active!
print("OpenTelemetry is ready!")
```

### 3. Use Instrumented Logging

Get OpenTelemetry-instrumented loggers for your application:

```python
from cezzis_otel import get_logger
import logging

# Get an instrumented logger
logger = get_logger(__name__, level=logging.INFO)

# Log messages will now include trace context and be exported
logger.info("Application started successfully")
logger.warning("This is a warning with trace context")
logger.error("Errors are automatically tracked")
```

### 4. Graceful Shutdown

Ensure proper cleanup when your application shuts down:

```python
from cezzis_otel import shutdown_otel

# At the end of your application
shutdown_otel()
```
### 5. Complete Example

Here's a complete example showing OpenTelemetry integration in a simple web service:

```python
from cezzis_otel import OTelSettings, initialize_otel, get_logger, shutdown_otel
import logging
import time

def main():
    # Configure OpenTelemetry
    settings = OTelSettings(
        service_name="example-service",
        service_namespace="demo",
        service_version="1.0.0",
        otlp_exporter_endpoint="http://localhost:4318",  # Local OTEL collector
        otlp_exporter_auth_header="",  # No auth for local development
        environment="development",
        instance_id="demo-instance"
    )
    
    # Initialize OpenTelemetry
    initialize_otel(settings)
    
    # Get instrumented logger
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting example service")
        
        # Simulate some work
        for i in range(5):
            logger.info(f"Processing item {i}")
            time.sleep(1)
            
        logger.info("Example service completed successfully")
        
    except Exception as e:
        logger.error(f"Service failed: {e}")
        raise
    finally:
        # Cleanup
        shutdown_otel()

if __name__ == "__main__":
    main()
```

## 📚 API Reference

### `OTelSettings`

Configuration class for OpenTelemetry setup.

**Attributes:**
- `service_name` (str): The name of your service (defaults to "unknown" if empty)
- `service_namespace` (str): The namespace/team owning the service (defaults to "unknown" if empty)
- `service_version` (str): Version of your service (defaults to "unknown" if empty)
- `otlp_exporter_endpoint` (str): OTLP collector endpoint URL (e.g., "http://localhost:4318")
- `otlp_exporter_auth_header` (str): Authorization header for OTLP exporter (e.g., "Bearer api-key")
- `environment` (str): Environment name (e.g., "production", "staging", "development")
- `instance_id` (str): Unique instance identifier (defaults to hostname if empty)
- `enable_logging` (bool): Enable OpenTelemetry logging integration (default: True)
- `enable_tracing` (bool): Enable OpenTelemetry tracing integration (default: True)

### `initialize_otel(settings, configure_tracing=None, configure_logging=None)`

Initialize OpenTelemetry tracing and logging with the provided settings.

**Parameters:**
- `settings` (OTelSettings): Configuration object for OpenTelemetry setup
- `configure_tracing` (Optional[Callable]): Optional callback to customize trace provider
- `configure_logging` (Optional[Callable]): Optional callback to customize log provider

**Example:**
```python
from cezzis_otel import initialize_otel, OTelSettings

settings = OTelSettings(
    service_name="my-service",
    service_namespace="production",
    service_version="1.0.0",
    otlp_exporter_endpoint="https://api.honeycomb.io",
    otlp_exporter_auth_header="Bearer your-key",
    environment="production",
    instance_id="pod-123"
)

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
logger.info("This message includes trace context")
```

### `shutdown_otel()`

Gracefully shutdown OpenTelemetry providers and flush any pending telemetry data.

**Example:**
```python
from cezzis_otel import shutdown_otel

# At application shutdown
shutdown_otel()
```

## 🛠️ Development

### Prerequisites

- Python 3.12+
- Poetry
- Docker (optional, for local opentelemetry collectors and observability platforms)

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/mtnvencenzo/cezzis-pycore.git
cd cezzis-pycore/cezzis-otel

# Install dependencies
make install

# Activate virtual environment
poetry shell
```

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
pytest --cov=cezzis_otel --cov-report=html
```

### Code Quality

```bash
# Run linting and formatting
make standards

# Run individually
make ruff-check    # Check code style
make ruff-format   # Format code
```

### Build Package

```bash
# Build distribution packages
poetry build
```

## 🧪 Testing with Local OpenTelemetry

### Using Docker Compose for OTEL Collector

```bash
# Create a simple docker-compose.yml for local testing
cat > docker-compose.yml << EOF
version: '3.8'
services:
  # OpenTelemetry Collector
  otel-collector:
    image: otel/opentelemetry-collector:latest
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml
    ports:
      - "4317:4317"   # OTLP gRPC receiver
      - "4318:4318"   # OTLP HTTP receiver
      - "8888:8888"   # Prometheus metrics
      - "8889:8889"   # Prometheus exporter metrics
    depends_on:
      - jaeger

  # Jaeger for trace visualization
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686" # Jaeger UI
      - "14250:14250" # Jaeger gRPC
EOF

# Create OTEL collector configuration
cat > otel-collector-config.yaml << EOF
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true
  logging:
    loglevel: debug

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger, logging]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging]
EOF

# Start the stack
docker-compose up -d

# View traces at http://localhost:16686
```

### Example with Local OTEL Setup

```python
from cezzis_otel import OTelSettings, initialize_otel, get_logger, shutdown_otel
import time

# Configure for local development
settings = OTelSettings(
    service_name="local-test-service",
    service_namespace="development",
    service_version="0.1.0",
    otlp_exporter_endpoint="http://localhost:4318",  # Local collector
    otlp_exporter_auth_header="",  # No auth needed locally
    environment="local",
    instance_id="dev-machine"
)

# Initialize OpenTelemetry
initialize_otel(settings)
logger = get_logger(__name__)

try:
    logger.info("Starting local test")
    
    # Generate some test traces and logs
    for i in range(3):
        logger.info(f"Processing item {i}")
        time.sleep(0.5)
        
    logger.info("Local test completed")
    
finally:
    shutdown_otel()
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](../.github/CONTRIBUTING.md) for details.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and linting (`make test && make standards`)
5. Commit your changes (`git commit -m 'feat: add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

## 🔗 Links

- **Documentation**: [Coming Soon]
- **Issue Tracker**: [GitHub Issues](https://github.com/mtnvencenzo/cezzis-pycore/issues)
- **Source Code**: [GitHub](https://github.com/mtnvencenzo/cezzis-pycore)

## 📞 Support

- 📧 Email: rvecchi@gmail.com
- 🐛 Issues: [GitHub Issues](https://github.com/mtnvencenzo/cezzis-pycore/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/mtnvencenzo/cezzis-pycore/discussions)

## 🙏 Acknowledgments

Built with:
- [OpenTelemetry Python](https://github.com/open-telemetry/opentelemetry-python) - The OpenTelemetry implementation for Python
- [Poetry](https://python-poetry.org/) - Dependency management and packaging

---

**Made with ❤️ by the Cezzis team**
