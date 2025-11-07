# Cezzis Kafka

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/cezzis-kafka.svg)](https://pypi.org/project/cezzis-kafka/)

A lightweight, production-ready Python library for Apache Kafka message consumption. Simplifies building Kafka consumers with built-in error handling, multi-process support, and structured logging.

## Installation

Install `cezzis-kafka` from PyPI:

```bash
pip install cezzis-kafka
```

Or using Poetry:

```bash
poetry add cezzis-kafka
```

## Requirements

- Python 3.12 or higher
- Apache Kafka cluster (local or remote)

## Source Code

Find the source code, contribute, or report issues on GitHub:

**Repository:** [https://github.com/mtnvencenzo/cezzis-pycore](https://github.com/mtnvencenzo/cezzis-pycore)

## Key Features

- **Simple Consumer API** - Minimal boilerplate to get started with Kafka consumption
- **Abstract Processor Interface** - Clean separation between Kafka operations and business logic
- **Multi-Process Support** - Built-in parallel consumer spawning for high throughput
- **Robust Error Handling** - Automatic error detection and lifecycle hooks
- **Structured Logging** - Rich context for debugging and monitoring
- **Type-Safe** - Full type hints for better IDE support and code quality

## Quick Start Guide

### Basic Example: Single Consumer

Here's a minimal example to get started with a single Kafka consumer:

```python
from cezzis_kafka import IKafkaMessageProcessor, KafkaConsumerSettings, start_consumer
from confluent_kafka import Consumer, Message
from multiprocessing import Event

class SimpleProcessor(IKafkaMessageProcessor):
    """Basic message processor that prints received messages."""
    
    def __init__(self, settings: KafkaConsumerSettings):
        self._settings = settings
    
    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "SimpleProcessor":
        return SimpleProcessor(kafka_settings)
    
    def kafka_settings(self) -> KafkaConsumerSettings:
        return self._settings
    
    def consumer_creating(self) -> None:
        print(f"[Consumer {self._settings.consumer_id}] Initializing...")
    
    def consumer_created(self, consumer: Consumer | None) -> None:
        if consumer:
            print(f"[Consumer {self._settings.consumer_id}] Ready!")
    
    def message_received(self, msg: Message) -> None:
        """Process the message - this is where your business logic goes."""
        value = msg.value().decode('utf-8')
        key = msg.key().decode('utf-8') if msg.key() else None
        print(f"[Consumer {self._settings.consumer_id}] Received: key={key}, value={value}")
    
    def message_error_received(self, msg: Message) -> None:
        print(f"[Consumer {self._settings.consumer_id}] Error: {msg.error()}")
    
    def consumer_subscribed(self) -> None:
        print(f"[Consumer {self._settings.consumer_id}] Subscribed to {self._settings.topic_name}")
    
    def consumer_stopping(self) -> None:
        print(f"[Consumer {self._settings.consumer_id}] Shutting down...")
    
    def message_partition_reached(self, msg: Message) -> None:
        print(f"[Consumer {self._settings.consumer_id}] Reached end of partition {msg.partition()}")

# Configure the consumer
settings = KafkaConsumerSettings(
    consumer_id=1,
    bootstrap_servers="localhost:9092",
    consumer_group="my-app-group",
    topic_name="orders",
    num_consumers=1
)

# Create processor and start consuming
processor = SimpleProcessor.CreateNew(settings)
stop_event = Event()

# This will block and consume messages until stop_event is set
start_consumer(stop_event, processor)
```

### Example: Multi-Process Consumer Pool

For high-throughput scenarios, spawn multiple consumer processes that share the workload:

```python
from cezzis_kafka import IKafkaMessageProcessor, KafkaConsumerSettings, spawn_consumers
from confluent_kafka import Consumer, Message
from multiprocessing import Event
import json

class OrderProcessor(IKafkaMessageProcessor):
    """Process order messages in parallel."""
    
    def __init__(self, settings: KafkaConsumerSettings):
        self._settings = settings
    
    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "OrderProcessor":
        return OrderProcessor(kafka_settings)
    
    def kafka_settings(self) -> KafkaConsumerSettings:
        return self._settings
    
    def consumer_creating(self) -> None:
        pass
    
    def consumer_created(self, consumer: Consumer | None) -> None:
        pass
    
    def message_received(self, msg: Message) -> None:
        # Parse JSON message
        order = json.loads(msg.value().decode('utf-8'))
        
        # Process the order
        print(f"Processing order {order['order_id']} on consumer {self._settings.consumer_id}")
        
        # Your business logic here
        # e.g., save to database, call external API, etc.
    
    def message_error_received(self, msg: Message) -> None:
        print(f"Error processing message: {msg.error()}")
    
    def consumer_subscribed(self) -> None:
        pass
    
    def consumer_stopping(self) -> None:
        print(f"Consumer {self._settings.consumer_id} cleanup complete")
    
    def message_partition_reached(self, msg: Message) -> None:
        pass

# Spawn 5 consumer processes
stop_event = Event()

spawn_consumers(
    factory_type=OrderProcessor,
    num_consumers=5,
    stop_event=stop_event,
    bootstrap_servers="localhost:9092",
    consumer_group="order-processing-group",
    topic_name="orders"
)
```

### Example: Graceful Shutdown

Handle shutdown signals properly for clean consumer termination:

```python
from cezzis_kafka import spawn_consumers, IKafkaMessageProcessor, KafkaConsumerSettings
from confluent_kafka import Consumer, Message
from multiprocessing import Event
import signal
import sys

class GracefulProcessor(IKafkaMessageProcessor):
    def __init__(self, settings: KafkaConsumerSettings):
        self._settings = settings
    
    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "GracefulProcessor":
        return GracefulProcessor(kafka_settings)
    
    def kafka_settings(self) -> KafkaConsumerSettings:
        return self._settings
    
    def consumer_creating(self) -> None:
        pass
    
    def consumer_created(self, consumer: Consumer | None) -> None:
        pass
    
    def message_received(self, msg: Message) -> None:
        # Process message
        print(f"Processing: {msg.value().decode('utf-8')}")
    
    def message_error_received(self, msg: Message) -> None:
        pass
    
    def consumer_subscribed(self) -> None:
        pass
    
    def consumer_stopping(self) -> None:
        print("Consumer cleanup...")
    
    def message_partition_reached(self, msg: Message) -> None:
        pass

# Setup graceful shutdown
stop_event = Event()

def signal_handler(sig, frame):
    print("\nShutdown signal received, stopping consumers...")
    stop_event.set()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Start consumers
spawn_consumers(
    factory_type=GracefulProcessor,
    num_consumers=3,
    stop_event=stop_event,
    bootstrap_servers="localhost:9092",
    consumer_group="graceful-group",
    topic_name="messages"
)
```

## API Reference

### `KafkaConsumerSettings`

Configuration dataclass for Kafka consumers.

**Parameters:**
- `consumer_id` (int): Unique identifier for this consumer instance (required)
- `bootstrap_servers` (str): Comma-separated Kafka broker addresses (required)
- `consumer_group` (str): Consumer group ID for coordinated message consumption (required)
- `topic_name` (str): Kafka topic to consume messages from (required)
- `num_consumers` (int): Total number of consumers in the group (default: 1)

**Example:**
```python
settings = KafkaConsumerSettings(
    consumer_id=1,
    bootstrap_servers="kafka1:9092,kafka2:9092",
    consumer_group="my-service",
    topic_name="user-events",
    num_consumers=3
)
```

### `IKafkaMessageProcessor`

Abstract base class that defines the interface for message processing. Implement all methods to create a custom processor.

**Required Methods:**

- **`CreateNew(kafka_settings: KafkaConsumerSettings) -> IKafkaMessageProcessor`**  
  Static factory method to create new processor instances.

- **`kafka_settings() -> KafkaConsumerSettings`**  
  Returns the consumer configuration.

- **`consumer_creating() -> None`**  
  Lifecycle hook called before consumer creation. Use for initialization (database connections, etc.).

- **`consumer_created(consumer: Consumer | None) -> None`**  
  Lifecycle hook called after consumer creation. Use to verify consumer setup.

- **`message_received(msg: Message) -> None`**  
  **Main processing method.** Called for each successfully received message. Implement your business logic here.

- **`message_error_received(msg: Message) -> None`**  
  Called when a message contains an error. Handle Kafka-level errors here.

- **`consumer_subscribed() -> None`**  
  Lifecycle hook called after successful topic subscription.

- **`consumer_stopping() -> None`**  
  Lifecycle hook called before consumer shutdown. Use for cleanup (close connections, flush buffers, etc.).

- **`message_partition_reached(msg: Message) -> None`**  
  Called when reaching the end of a partition (EOF event).

### `start_consumer(stop_event, processor)`

Starts a single Kafka consumer that continuously polls and processes messages.

**Parameters:**
- `stop_event` (Event): Multiprocessing Event to signal consumer shutdown
- `processor` (IKafkaMessageProcessor): Processor instance that handles messages

**Behavior:**
- Blocks until `stop_event` is set
- Handles consumer lifecycle automatically
- Commits offsets after successful message processing
- Gracefully handles `KeyboardInterrupt` and exceptions

**Example:**
```python
from multiprocessing import Event

processor = MyProcessor.CreateNew(settings)
stop_event = Event()

try:
    start_consumer(stop_event, processor)
except KeyboardInterrupt:
    stop_event.set()
```

### `spawn_consumers(factory_type, num_consumers, stop_event, bootstrap_servers, consumer_group, topic_name)`

Spawns multiple consumer processes that work together in the same consumer group to process messages in parallel.

**Parameters:**
- `factory_type` (Type[IKafkaMessageProcessor]): Processor class with `CreateNew` factory method
- `num_consumers` (int): Number of consumer processes to spawn
- `stop_event` (Event): Multiprocessing Event to signal shutdown across all consumers
- `bootstrap_servers` (str): Kafka broker addresses
- `consumer_group` (str): Consumer group ID (all spawned consumers share this group)
- `topic_name` (str): Topic to consume from

**Behavior:**
- Creates `num_consumers` separate processes
- Assigns unique `consumer_id` (0 to num_consumers-1) to each
- Kafka automatically distributes partitions among consumers in the group
- Waits for all processes to complete
- Logs startup and shutdown events

**Example:**
```python
from multiprocessing import Event

stop_event = Event()

# Spawn 4 consumers in the same group
spawn_consumers(
    factory_type=MyProcessor,
    num_consumers=4,
    stop_event=stop_event,
    bootstrap_servers="localhost:9092",
    consumer_group="parallel-processors",
    topic_name="high-volume-topic"
)
```

## Testing Locally

### Quick Kafka Setup with Docker

Run a local Kafka instance for development and testing:

```bash
# Start Kafka with KRaft (no Zookeeper needed)
docker run -d \
  --name kafka-local \
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
docker exec kafka-local kafka-topics.sh \
  --create \
  --topic test-messages \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Produce test messages
docker exec -it kafka-local kafka-console-producer.sh \
  --topic test-messages \
  --bootstrap-server localhost:9092

# View messages (in another terminal)
docker exec -it kafka-local kafka-console-consumer.sh \
  --topic test-messages \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

## Common Use Cases

### Event-Driven Microservices

Process events from other services in your architecture:

```python
class UserEventProcessor(IKafkaMessageProcessor):
    def message_received(self, msg: Message) -> None:
        event = json.loads(msg.value().decode('utf-8'))
        
        if event['type'] == 'user.created':
            self._send_welcome_email(event['user_id'])
        elif event['type'] == 'user.deleted':
            self._cleanup_user_data(event['user_id'])
```

### Data Pipeline / ETL

Extract, transform, and load data from Kafka to databases or data warehouses:

```python
class ETLProcessor(IKafkaMessageProcessor):
    def consumer_creating(self) -> None:
        self.db = connect_to_database()
    
    def message_received(self, msg: Message) -> None:
        raw_data = json.loads(msg.value().decode('utf-8'))
        transformed = self._transform(raw_data)
        self.db.insert(transformed)
    
    def consumer_stopping(self) -> None:
        self.db.close()
```

### Real-Time Analytics

Process streams for metrics, aggregations, or alerting:

```python
class AnalyticsProcessor(IKafkaMessageProcessor):
    def __init__(self, settings: KafkaConsumerSettings):
        super().__init__(settings)
        self.metrics_client = MetricsClient()
    
    def message_received(self, msg: Message) -> None:
        event = json.loads(msg.value().decode('utf-8'))
        self.metrics_client.increment(f"events.{event['type']}")
        
        if event.get('value', 0) > THRESHOLD:
            self._send_alert(event)
```

## Best Practices

### 1. Error Handling

Always handle exceptions in `message_received` to prevent consumer crashes:

```python
def message_received(self, msg: Message) -> None:
    try:
        # Your processing logic
        process_message(msg)
    except Exception as e:
        logger.error(f"Failed to process message: {e}", exc_info=True)
        # Optionally send to dead letter queue
        self._send_to_dlq(msg)
```

### 2. Idempotency

Design processors to handle duplicate messages gracefully:

```python
def message_received(self, msg: Message) -> None:
    message_id = msg.key().decode('utf-8')
    
    # Check if already processed
    if self.cache.exists(message_id):
        logger.info(f"Skipping duplicate message: {message_id}")
        return
    
    # Process and mark as complete
    process_message(msg)
    self.cache.set(message_id, True, ttl=86400)
```

### 3. Resource Cleanup

Always clean up resources in `consumer_stopping`:

```python
def consumer_creating(self) -> None:
    self.db_connection = create_connection()
    self.cache = RedisCache()

def consumer_stopping(self) -> None:
    if hasattr(self, 'db_connection'):
        self.db_connection.close()
    if hasattr(self, 'cache'):
        self.cache.disconnect()
```

### 4. Monitoring and Logging

Use structured logging for better observability:

```python
import logging

logger = logging.getLogger(__name__)

def message_received(self, msg: Message) -> None:
    logger.info(
        "Processing message",
        extra={
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "consumer_id": self._settings.consumer_id
        }
    )
```

## Troubleshooting

### Consumer Not Receiving Messages

1. Verify Kafka is running: `docker ps` or check your Kafka cluster
2. Check topic exists: `kafka-topics.sh --list --bootstrap-server localhost:9092`
3. Verify consumer group: Different groups get independent copies of messages
4. Check partition assignment: Multiple consumers may split partitions

### High Memory Usage

- Reduce `num_consumers` if spawning too many processes
- Implement batching in your processor to reduce per-message overhead
- Monitor partition lag to ensure consumers are keeping up

### Messages Processed Multiple Times

- Ensure proper error handling to prevent consumer crashes mid-processing
- Use idempotency keys to track processed messages
- Configure appropriate commit intervals in Confluent Kafka settings

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

Built with [Confluent Kafka Python](https://github.com/confluentinc/confluent-kafka-python), the official Python client for Apache Kafka.

---

**Happy streaming! 🚀**
