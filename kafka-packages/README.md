# 🚀 Cezzis Kafka

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A lightweight, production-ready Python library for working with Apache Kafka. Simplifies message consumption and processing with built-in error handling, multi-process support, and structured logging.

## ✨ Features

- 🔄 **Easy Consumer Management** - Simple, intuitive API for Kafka message consumption
- 🏗️ **Abstract Processor Interface** - Clean separation of concerns with `IKafkaMessageProcessor`
- ⚡ **Multi-Process Support** - Built-in support for parallel consumer processes
- 🛡️ **Robust Error Handling** - Comprehensive error handling with automatic retries
- 📊 **Structured Logging** - Rich, contextual logging for observability
- 🔌 **Confluent Kafka Integration** - Built on the reliable `confluent-kafka` client

## 📦 Installation

### Using Poetry (Recommended)

```bash
poetry add cezzis-kafka
```

### Using pip

```bash
pip install cezzis-kafka
```

## 🚀 Quick Start

### 1. Create Your Message Processor

Implement the `IKafkaMessageProcessor` interface to define how messages should be processed:

```python
from cezzis_kafka import IKafkaMessageProcessor, KafkaConsumerSettings
from confluent_kafka import Consumer, Message

class MyMessageProcessor(IKafkaMessageProcessor):
    def __init__(self, settings: KafkaConsumerSettings):
        self._settings = settings
    
    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "MyMessageProcessor":
        return MyMessageProcessor(kafka_settings)
    
    def kafka_settings(self) -> KafkaConsumerSettings:
        return self._settings
    
    def consumer_creating(self) -> None:
        """Handle actions when consumer is being created."""
        print("Creating consumer...")
    
    def consumer_created(self, consumer: Consumer | None) -> None:
        """Handle actions when consumer has been created."""
        print(f"Consumer created: {consumer}")
    
    def message_received(self, msg: Message) -> None:
        """Process a received Kafka message."""
        print(f"Processing: {msg.value().decode('utf-8')}")
    
    def message_error_received(self, msg: Message) -> None:
        """Handle message errors."""
        print(f"Error in message: {msg.error()}")
    
    def consumer_subscribed(self) -> None:
        """Handle actions when consumer is subscribed."""
        print("Consumer subscribed to topic")
    
    def consumer_stopping(self) -> None:
        """Handle actions when consumer is stopping."""
        print("Consumer stopping...")
    
    def message_partition_reached(self, msg: Message) -> None:
        """Handle partition EOF events."""
        print(f"Reached end of partition: {msg.partition()}")
```

### 2. Configure and Start the Consumer

```python
from cezzis_kafka import KafkaConsumerSettings, start_consumer
from multiprocessing import Event

# Configure Kafka settings
settings = KafkaConsumerSettings(
    consumer_id=1,
    bootstrap_servers="localhost:9092",
    consumer_group="my-consumer-group",
    topic_name="my-topic",
    num_consumers=1
)

# Create processor instance
processor = MyMessageProcessor.CreateNew(settings)

# Start consuming messages
stop_event = Event()
start_consumer(stop_event, processor)
```

### 3. Multi-Process Consumer

Run multiple consumer processes for better throughput using `spawn_consumers`:

```python
from cezzis_kafka import spawn_consumers
from multiprocessing import Event

# Create a stop event for graceful shutdown
stop_event = Event()

# Spawn 3 consumer processes
spawn_consumers(
    factory_type=MyMessageProcessor,
    num_consumers=3,
    stop_event=stop_event,
    bootstrap_servers="localhost:9092",
    consumer_group="my-consumer-group",
    topic_name="my-topic"
)
```

The `spawn_consumers` function will:
- Create the specified number of consumer processes
- Assign each a unique `consumer_id` (0, 1, 2, ...)
- Start all processes and wait for them to complete
- Handle process lifecycle and logging automatically

## 📚 API Reference

### `KafkaConsumerSettings`

Configuration class for Kafka consumers.

**Attributes:**
- `consumer_id` (int): Unique identifier for the consumer instance
- `bootstrap_servers` (str): Comma-separated list of Kafka broker addresses
- `consumer_group` (str): Consumer group ID for coordinated consumption
- `topic_name` (str): Name of the Kafka topic to consume from
- `num_consumers` (int): Number of consumer processes to run

### `IKafkaMessageProcessor`

Abstract base class for implementing custom message processors.

**Abstract Methods:**

- `CreateNew(kafka_settings) -> IKafkaMessageProcessor` - Factory method for creating processor instances
- `kafka_settings() -> KafkaConsumerSettings` - Returns the Kafka consumer settings
- `consumer_creating() -> None` - Lifecycle hook called when consumer is being created
- `consumer_created(consumer: Consumer | None) -> None` - Lifecycle hook called when consumer has been created
- `message_received(msg: Message) -> None` - Process a received Kafka message
- `message_error_received(msg: Message) -> None` - Handle errors in received messages
- `consumer_subscribed() -> None` - Lifecycle hook called when consumer subscribes to topic
- `consumer_stopping() -> None` - Lifecycle hook called when consumer is stopping
- `message_partition_reached(msg: Message) -> None` - Handle partition EOF events

### `spawn_consumers(factory_type, num_consumers, stop_event, bootstrap_servers, consumer_group, topic_name)`

Spawns multiple Kafka consumer processes under a single consumer group for parallel message processing.

**Parameters:**
- `factory_type` (Type[IKafkaMessageProcessor]): The processor class with a `CreateNew` factory method
- `num_consumers` (int): Number of consumer processes to spawn
- `stop_event` (Event): Multiprocessing event to signal consumer shutdown
- `bootstrap_servers` (str): Comma-separated list of Kafka broker addresses
- `consumer_group` (str): Consumer group ID for coordinated consumption
- `topic_name` (str): Name of the Kafka topic to consume from

**Example:**
```python
spawn_consumers(
    factory_type=MyMessageProcessor,
    num_consumers=3,
    stop_event=stop_event,
    bootstrap_servers="localhost:9092",
    consumer_group="my-group",
    topic_name="my-topic"
)
```

### `start_consumer(stop_event, processor)`

Starts a single Kafka consumer that polls for messages and processes them using the provided processor.

**Parameters:**
- `stop_event` (Event): Multiprocessing event to signal consumer shutdown
- `processor` (IKafkaMessageProcessor): Message processor implementation

**Example:**
```python
processor = MyMessageProcessor.CreateNew(settings)
start_consumer(stop_event, processor)
```

## 🛠️ Development

### Prerequisites

- Python 3.12+
- Poetry
- Docker (optional, for local Kafka)

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/mtnvencenzo/cezzis-pycore.git
cd cezzis-pycore/kafka-packages

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
pytest --cov=cezzis_kafka --cov-report=html
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

## 🧪 Testing with Local Kafka

### Using Docker Compose

```bash
# Start local Kafka cluster
docker run -d \
  --name kafka \
  -p 9092:9092 \
  -e KAFKA_ENABLE_KRAFT=yes \
  -e KAFKA_CFG_PROCESS_ROLES=broker,controller \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  bitnami/kafka:latest

# Create a test topic
docker exec kafka kafka-topics.sh \
  --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
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
- [Confluent Kafka Python](https://github.com/confluentinc/confluent-kafka-python) - The underlying Kafka client
- [Poetry](https://python-poetry.org/) - Dependency management and packaging

---

**Made with ❤️ by the Cezzis team**
