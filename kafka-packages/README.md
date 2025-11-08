# 🚀 Cezzis Kafka

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A comprehensive, production-ready Python library for Apache Kafka. Provides both **consumer** and **producer** functionality with enterprise-grade features including retry mechanisms, dead letter queues (DLQ), delivery tracking, and robust error handling.

## ✨ Features

### 🔄 **Consumer Features**
- Easy Consumer Management with intuitive API
- Abstract Processor Interface for clean separation of concerns
- Multi-Process Support for parallel message processing
- Robust Error Handling with automatic retries
- Structured Logging for comprehensive observability

### 📤 **Producer Features** 
- **Enterprise Delivery Handler** - Advanced retry logic with exponential backoff
- **Dead Letter Queue (DLQ)** - Automatic routing of failed messages
- **Delivery Tracking** - Comprehensive delivery status monitoring
- **Settings-Based Configuration** - Consistent, validated configuration management
- **Metrics Integration** - Built-in hooks for monitoring and observability
- **Thread-Safe Operations** - Concurrent publishing with proper resource management

### 🛡️ **Shared Features**
- Built on reliable Confluent Kafka client
- Comprehensive error classification and handling
- Production-ready with extensive test coverage (207+ tests)
- Type hints and comprehensive documentation

## � Table of Contents

- [📦 Installation](#-installation)
- [🚀 Quick Start](#-quick-start)
  - [🔽 Consumer Quick Start](#-consumer-quick-start) 
  - [📤 Producer Quick Start](#-producer-quick-start)
- [🔽 Consumer Guide](#-consumer-guide)
  - [Creating Message Processors](#creating-message-processors)
  - [Multi-Process Consumption](#multi-process-consumption)
- [📤 Producer Guide](#-producer-guide)
  - [Basic Publishing](#basic-publishing)
  - [Advanced Features](#advanced-features)
  - [Enterprise Delivery Handling](#enterprise-delivery-handling)
- [📚 API Reference](#-api-reference)
- [🛠️ Development](#️-development)
- [🧪 Testing](#-testing)

## �📦 Installation

### Using Poetry (Recommended)

```bash
poetry add cezzis-kafka
```

### Using pip

```bash
pip install cezzis-kafka
```

## 🚀 Quick Start

### 🔽 Consumer Quick Start

<details>
<summary>Click to expand consumer quick start example</summary>

#### 1. Create Your Message Processor

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

#### 2. Configure and Start the Consumer

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

</details>




## 🔽 Consumer Guide

### Creating Message Processors

The `IKafkaMessageProcessor` interface provides lifecycle hooks for comprehensive message handling:

```python
from cezzis_kafka import IKafkaMessageProcessor, KafkaConsumerSettings
from confluent_kafka import Consumer, Message
import json

class OrderProcessor(IKafkaMessageProcessor):
    def __init__(self, settings: KafkaConsumerSettings):
        self._settings = settings
        self._processed_count = 0
    
    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "OrderProcessor":
        return OrderProcessor(kafka_settings)
    
    def kafka_settings(self) -> KafkaConsumerSettings:
        return self._settings
    
    def message_received(self, msg: Message) -> None:
        """Process order messages."""
        try:
            order_data = json.loads(msg.value().decode('utf-8'))
            self._process_order(order_data)
            self._processed_count += 1
            print(f"Processed order {order_data.get('id')} - Total: {self._processed_count}")
        except json.JSONDecodeError:
            print(f"Invalid JSON in message: {msg.value()}")
        except Exception as e:
            print(f"Error processing order: {e}")
    
    def _process_order(self, order_data: dict) -> None:
        # Your business logic here
        pass
```

### Multi-Process Consumption

For high-throughput scenarios, run multiple consumer processes:

```python
# multi_consumer.py
from multiprocessing import Process, Event
import signal
import sys

def run_consumer_process(consumer_id: int, shared_stop_event: Event):
    """Run a consumer in a separate process."""
    from cezzis_kafka import KafkaConsumerSettings, start_consumer
    
    settings = KafkaConsumerSettings(
        consumer_id=consumer_id,
        bootstrap_servers="localhost:9092",
        consumer_group="order-processing-group",
        topic_name="orders",
        num_consumers=1
    )
    
    processor = OrderProcessor.CreateNew(settings)
    start_consumer(shared_stop_event, processor)

if __name__ == "__main__":
    # Number of consumer processes
    num_processes = 4
    shared_stop_event = Event()
    
    # Start consumer processes
    processes = []
    for i in range(num_processes):
        p = Process(target=run_consumer_process, args=(i, shared_stop_event))
        p.start()
        processes.append(p)
        print(f"Started consumer process {i}")
    
    def signal_handler(sig, frame):
        print("\nShutting down consumers...")
        shared_stop_event.set()
        for p in processes:
            p.join(timeout=10)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Wait for processes
    for p in processes:
        p.join()
```

</details>

## 🔼 Producer Guide

The Kafka producer enables high-performance, reliable message publishing with enterprise-grade features including automatic retries, dead letter queues (DLQ), and comprehensive error handling.

### 📤 Producer Quick Start

<details>
<summary>Click to expand producer quick start example</summary>

#### 1. Basic Message Publishing

```python
from cezzis_kafka import KafkaPublisher, KafkaPublisherSettings

# Configure publisher settings
settings = KafkaPublisherSettings(
    bootstrap_servers="localhost:9092"
)

# Create publisher
publisher = KafkaPublisher(settings)

# Send a simple message
publisher.send(
    topic="my-topic",
    message="Hello, Kafka!",
    message_id="msg-001"  # Optional: auto-generated if not provided
)

# Send with headers and key
publisher.send(
    topic="user-events",
    message={"user_id": 123, "action": "login"},
    key="user-123",
    headers={"content-type": "application/json", "version": "v1"}
)

# Ensure all messages are sent
publisher.flush()
publisher.close()
```

</details>

### Enterprise Publishing with Retry & DLQ

For production environments, configure automatic retries and dead letter queue handling:

```python
from cezzis_kafka import KafkaPublisher, KafkaPublisherSettings, DeliveryHandler
from cezzis_kafka.delivery_handler import DeliveryContext, DeliveryStatus
import json

# Enterprise producer configuration
settings = KafkaPublisherSettings(
    bootstrap_servers="localhost:9092",
    topic_name="orders",
    client_id="order-service",
    
    # Kafka producer optimizations
    acks="all",                    # Wait for all replicas
    retries=3,                     # Kafka-level retries
    batch_size=16384,              # Batch messages for efficiency
    linger_ms=10,                  # Wait up to 10ms to batch
    compression_type="snappy",     # Compress messages
    max_in_flight_requests_per_connection=5,
    
    # DLQ configuration
    dlq_topic_name="orders-dlq",
    
    # Retry settings
    enable_retry=True,
    max_retry_attempts=3,
    base_delay_seconds=1.0,        # Start with 1 second delay
    max_delay_seconds=30.0,        # Cap at 30 seconds
    backoff_multiplier=2.0         # Exponential backoff
)

# Custom delivery handler for monitoring
class OrderDeliveryHandler(DeliveryHandler):
    def __init__(self, settings: KafkaPublisherSettings):
        super().__init__(settings)
        self._success_count = 0
        self._failure_count = 0
    
    def on_delivery_success(self, context: DeliveryContext) -> None:
        super().on_delivery_success(context)
        self._success_count += 1
        print(f"✅ Order {context.key} delivered successfully")
    
    def on_delivery_failure(self, context: DeliveryContext, error: Exception) -> None:
        self._failure_count += 1
        print(f"❌ Order {context.key} delivery failed: {error}")
        super().on_delivery_failure(context, error)
    
    def on_retry_scheduled(self, context: DeliveryContext, delay: float, attempt: int) -> None:
        print(f"🔄 Retry {attempt} scheduled for order {context.key} in {delay:.1f}s")
    
    def on_dlq_sent(self, context: DeliveryContext, error: Exception) -> None:
        print(f"💀 Order {context.key} sent to DLQ after max retries: {error}")

# Initialize publisher with custom handler
publisher = KafkaPublisher(settings, delivery_handler=OrderDeliveryHandler(settings))

# Publish messages with automatic retry/DLQ
order_data = {"id": "12345", "product": "Premium Widget", "quantity": 5}
message = json.dumps(order_data)

try:
    future = publisher.send(key=f"order-{order_data['id']}", value=message)
    result = future.get(timeout=30)
    print(f"Message delivered to partition {result.partition}, offset {result.offset}")
except Exception as e:
    print(f"Final delivery failure: {e}")

# Graceful shutdown (important for retry cleanup)
publisher.close()
```

### Batch Publishing with Error Handling

For high-throughput scenarios:

```python
import asyncio
from concurrent.futures import as_completed

async def publish_order_batch(publisher: KafkaPublisher, orders: list):
    """Publish a batch of orders with concurrent processing."""
    futures = []
    
    for order in orders:
        message = json.dumps(order)
        future = publisher.send(
            key=f"order-{order['id']}", 
            value=message
        )
        futures.append((future, order['id']))
    
    # Wait for all deliveries with timeout
    successful = 0
    failed = 0
    
    for future, order_id in futures:
        try:
            result = future.get(timeout=30)
            successful += 1
            print(f"Order {order_id}: partition={result.partition}, offset={result.offset}")
        except Exception as e:
            failed += 1
            print(f"Order {order_id} failed: {e}")
    
    return {"successful": successful, "failed": failed}

# Example usage
orders = [
    {"id": "1001", "product": "Widget A", "quantity": 5},
    {"id": "1002", "product": "Widget B", "quantity": 3},
    {"id": "1003", "product": "Widget C", "quantity": 8},
    # ... more orders
]

stats = asyncio.run(publish_order_batch(publisher, orders))
print(f"Batch complete: {stats['successful']} successful, {stats['failed']} failed")
```

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

### 🔼 Producer API Reference

### `KafkaPublisherSettings`

Configuration class for Kafka publishers with comprehensive settings for production use.

**Core Attributes:**
- `bootstrap_servers` (str): Comma-separated list of Kafka broker addresses
- `topic_name` (str): Default topic name for message publishing
- `client_id` (str, optional): Unique identifier for the producer client

**Kafka Producer Configuration:**
- `acks` (str, default="1"): Acknowledgment level ("0", "1", "all")
- `retries` (int, default=3): Number of Kafka-level retries
- `batch_size` (int, default=16384): Number of bytes to batch before sending
- `linger_ms` (int, default=0): Time to wait for batching (milliseconds)
- `compression_type` (str, optional): Compression algorithm ("gzip", "snappy", "lz4", "zstd")
- `max_in_flight_requests_per_connection` (int, default=5): Maximum unacknowledged requests

**Retry & DLQ Configuration:**
- `enable_retry` (bool, default=False): Enable application-level retry mechanism
- `max_retry_attempts` (int, default=3): Maximum number of retry attempts
- `base_delay_seconds` (float, default=1.0): Initial retry delay
- `max_delay_seconds` (float, default=60.0): Maximum retry delay cap
- `backoff_multiplier` (float, default=2.0): Exponential backoff multiplier
- `dlq_topic_name` (str, optional): Dead letter queue topic name

### `KafkaPublisher`

High-performance Kafka producer with enterprise features.

**Constructor:**
```python
KafkaPublisher(settings: KafkaPublisherSettings, delivery_handler: DeliveryHandler = None)
```

**Methods:**

- `send(topic: str = None, key: str = None, value: str = None, headers: dict = None, message_id: str = None) -> Future`: Send a message and return a Future for the delivery result
- `flush(timeout: float = None) -> None`: Flush all pending messages with optional timeout
- `close()`: Gracefully shutdown the producer with proper resource cleanup

### `DeliveryHandler`

Base class for handling message delivery callbacks with retry and DLQ functionality.

**Constructor:**
```python
DeliveryHandler(settings: KafkaPublisherSettings)
```

**Key Methods (Override for custom behavior):**

- `on_delivery_success(context: DeliveryContext) -> None`: Called on successful delivery
- `on_delivery_failure(context: DeliveryContext, error: Exception) -> None`: Called on delivery failure (triggers retry logic)
- `on_retry_scheduled(context: DeliveryContext, delay: float, attempt: int) -> None`: Called when retry is scheduled
- `on_dlq_sent(context: DeliveryContext, error: Exception) -> None`: Called when message is sent to DLQ
- `shutdown() -> None`: Cleanup method for graceful shutdown

**Built-in Error Classification:**
- **Retriable errors**: Network timeouts, broker unavailability, leader not available
- **Non-retriable errors**: Authentication failures, message too large, unknown topics
- **Terminal errors**: Serialization errors, invalid configurations

### `DeliveryContext`

Context object containing message delivery information.

**Attributes:**
- `key` (str): Message key
- `value` (str): Message value  
- `topic` (str): Destination topic
- `headers` (dict): Message headers
- `message_id` (str): Unique message identifier
- `attempt_count` (int): Current retry attempt number
- `status` (DeliveryStatus): Current delivery status

### `DeliveryStatus`

Enumeration of message delivery states.

**Values:**
- `PENDING`: Message queued for delivery
- `SUCCESS`: Message delivered successfully
- `FAILED`: Message delivery failed
- `RETRY_SCHEDULED`: Retry scheduled for failed message
- `DLQ_SENT`: Message sent to dead letter queue

## 🏢 Enterprise Features

### Automatic Retry Mechanism

The producer includes a sophisticated retry mechanism using `threading.Timer` for in-memory scheduling:

- **Exponential backoff**: Delays increase exponentially between retry attempts (1s, 2s, 4s, 8s...)
- **Configurable limits**: Set maximum retry attempts and delay caps
- **Error classification**: Intelligent handling of different error types (retriable vs terminal)
- **Thread safety**: Safe for concurrent use in multi-threaded applications

### Dead Letter Queue (DLQ) Support

Messages that exceed retry limits are automatically routed to a dead letter queue:

- **Automatic routing**: Terminal failures and retry exhaustion handled transparently
- **Error preservation**: Original error information and context preserved in DLQ messages
- **Configurable topics**: Specify custom DLQ topic names per publisher
- **Message enrichment**: DLQ messages include failure reason and retry history

### Error Classification System

Built-in intelligence for categorizing and handling different error types:

**Retriable Errors** (automatically retried):
- Network timeouts and connection issues
- Broker temporarily unavailable
- Leader not available for partition
- Request timeouts

**Non-Retriable Errors** (sent directly to DLQ):
- Authentication and authorization failures
- Message size exceeds broker limits
- Unknown or invalid topic names
- Serialization errors

### Metrics and Monitoring

Extensible metrics system for production monitoring:

```python
class MetricsDeliveryHandler(DeliveryHandler):
    def on_delivery_success(self, context: DeliveryContext) -> None:
        super().on_delivery_success(context)
        metrics.increment("kafka.messages.delivered.success")
        
    def on_delivery_failure(self, context: DeliveryContext, error: Exception) -> None:
        super().on_delivery_failure(context, error)
        metrics.increment("kafka.messages.delivered.failed")
        
    def on_dlq_sent(self, context: DeliveryContext, error: Exception) -> None:
        super().on_dlq_sent(context, error)
        metrics.increment("kafka.messages.dlq")
```

## 🎯 Best Practices

### Producer Configuration

**High Throughput:**
```python
settings = KafkaPublisherSettings(
    batch_size=32768,           # Larger batches
    linger_ms=50,               # Allow more batching time
    compression_type="lz4",     # Fast compression
    acks="1"                    # Balance reliability/performance
)
```

**High Reliability:**
```python
settings = KafkaPublisherSettings(
    acks="all",                 # Wait for all replicas
    retries=10,                 # More Kafka-level retries
    enable_retry=True,          # Application-level retries
    max_retry_attempts=5,       # Comprehensive retry coverage
    dlq_topic_name="critical-dlq"  # Capture all failures
)
```

**Low Latency:**
```python
settings = KafkaPublisherSettings(
    batch_size=1,               # Minimal batching
    linger_ms=0,                # Send immediately
    acks="1",                   # Fast acknowledgment
    compression_type=None       # No compression overhead
)
```

### Error Handling Strategies

1. **Graceful Degradation**: Implement fallback mechanisms for critical failures
2. **Circuit Breaker**: Stop publishing during sustained failures to prevent resource exhaustion
3. **Rate Limiting**: Control publish rates during recovery periods
4. **Health Checks**: Monitor producer health and delivery success rates

### Resource Management

- **Connection Pooling**: Reuse producer instances across your application
- **Graceful Shutdown**: Always call `publisher.close()` to ensure message delivery completion
- **Memory Management**: Monitor delivery handler state for long-running applications
- **Thread Safety**: DeliveryHandler is thread-safe and can handle concurrent deliveries

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
