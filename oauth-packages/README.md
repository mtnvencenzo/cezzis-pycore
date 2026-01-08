# 🚀 Cezzis Kafka

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![PyPI version](https://img.shields.io/pypi/v/cezzis-kafka.svg)](https://pypi.org/project/cezzis-kafka/)
[![CI/CD](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-kafka-cicd.yaml/badge.svg)](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-kafka-cicd.yaml)

A comprehensive, production-ready Python library for Apache Kafka. Provides both **consumer** and **producer** functionality with enterprise-grade features including built-in retry mechanisms, delivery tracking, and robust error handling.


## Installation

### Using Poetry (Recommended)

```bash
poetry add cezzis-kafka
```

### Using pip

```bash
pip install cezzis-kafka
```

## Kafka Consumer Guide

### Consumer Quick Start


#### Create Your Message Processor

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

#### Configure and Start the Consumer

```python
from cezzis_kafka import KafkaConsumerSettings, start_consumer
from multiprocessing import Event

# Configure Kafka settings
settings = KafkaConsumerSettings(
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



### Multi-Process Consumption

For high-throughput scenarios, run multiple consumer processes:

```python
# multi_consumer.py
from multiprocessing import Process, Event
import signal
import sys

def run_consumer_process(shared_stop_event: Event):
    """Run a consumer in a separate process."""
    from cezzis_kafka import KafkaConsumerSettings, start_consumer
    
    settings = KafkaConsumerSettings(
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


## Producer Guide

The Kafka producer enables high-performance, reliable message publishing with enterprise-grade features including automatic retries with built-in Kafka retry mechanisms and comprehensive error handling.

### Producer Quick Start

```python
from cezzis_kafka import KafkaProducer, KafkaProducerSettings

# Configure producer settings
settings = KafkaProducerSettings(
    bootstrap_servers="localhost:9092"
)

# Create producer
producer = KafkaProducer(settings)

# Send a simple message
producer.send(
    topic="my-topic",
    message="Hello, Kafka!",
)
```

### Synchronous Message Delivery

For scenarios where you need to ensure immediate message delivery and get confirmation:

```python
# Send and wait for delivery confirmation
try:
    message_id = producer.send_and_wait(
        topic="critical-topic",
        message="Important message",
        timeout=30.0  # Wait up to 30 seconds
    )
    print(f"Message {message_id} delivered successfully")
except Exception as e:
    print(f"Message delivery failed: {e}")
```


## Producer configuration
```python
settings = KafkaProducerSettings(
    bootstrap_servers="localhost:9092",
    max_retries=5,                    # Use Kafka's built-in retry mechanism
    retry_backoff_ms=200,             # Initial retry backoff 
    retry_backoff_max_ms=2000,        # Maximum retry backoff
    delivery_timeout_ms=600000,       # 10 minute total delivery timeout
    request_timeout_ms=60000,         # 1 minute request timeout
    on_delivery=delivery_callback,    # Optional delivery tracking
    producer_config={
        # Additional Kafka producer optimizations
        "batch.size": 32768,              # Larger batches for efficiency
        "linger.ms": 20,                  # Wait up to 20ms to batch
        "compression.type": "snappy",     # Compress messages
        "max.in.flight.requests.per.connection": 5,  # Concurrent requests
        "enable.idempotence": True,       # Prevent duplicates
        "acks": "all",                    # Wait for all replicas (most durable)
    }
)
```

### Best Practices for Message Delivery

#### Immediate Delivery Patterns

**Option 1: Use send_and_wait for critical messages**
```python
# Guaranteed delivery with timeout
producer.send_and_wait(
    topic="orders",
    message=order_data,
    timeout=30.0
)
```

**Option 2: Poll + Flush pattern**
```python
producer.send(topic="events", message=data)
producer.poll(0.1)  # Process delivery callbacks
producer.flush(timeout=30.0)  # Ensure delivery
```

**Option 3: Batch processing (most efficient)**
```python
# Send multiple messages
for item in batch_items:
    producer.send(topic="batch_topic", message=item)

# Flush once at the end
producer.flush(timeout=60.0)
```

#### Performance vs Reliability Trade-offs

**High Performance (less durable):**
```python
producer_config = {
    "acks": "1",  # Only leader acknowledgment
    "max.in.flight.requests.per.connection": 5,
    "enable.idempotence": False,
}
```

**High Reliability (slower):**
```python
producer_config = {
    "acks": "all",  # All replicas must acknowledge
    "max.in.flight.requests.per.connection": 1,
    "enable.idempotence": True,
}
```


## API Reference

### `KafkaConsumerSettings`

Configuration class for Kafka consumers.

**Attributes:**
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

### � Producer API Reference

### `KafkaProducerSettings`

Configuration class for Kafka producers with built-in retry mechanisms.

**Constructor:**
```python
KafkaProducerSettings(
    bootstrap_servers: str,
    max_retries: int = 3,
    retry_backoff_ms: int = 100,
    retry_backoff_max_ms: int = 1000,
    delivery_timeout_ms: int = 300000,
    request_timeout_ms: int = 30000,
    on_delivery: Optional[Callable[[KafkaError | None, Message], None]] = None,
    producer_config: Optional[Dict[str, Any]] = None
)
```

**Parameters:**
- `bootstrap_servers` (str): Comma-separated list of Kafka broker addresses
- `max_retries` (int, default=3): Maximum number of retries using Kafka's built-in retry mechanism
- `retry_backoff_ms` (int, default=100): Initial backoff time in milliseconds between retries
- `retry_backoff_max_ms` (int, default=1000): Maximum backoff time in milliseconds between retries  
- `delivery_timeout_ms` (int, default=300000): Total timeout for message delivery including retries (5 minutes)
- `request_timeout_ms` (int, default=30000): Timeout for individual produce requests (30 seconds)
- `on_delivery` (Optional[Callable]): Optional callback function for delivery reports
- `producer_config` (Optional[Dict]): Additional Kafka producer configuration to override defaults

### `KafkaProducer`

High-performance Kafka producer leveraging Kafka's built-in retry mechanisms and enterprise features.

**Constructor:**
```python
KafkaProducer(settings: KafkaProducerSettings)
```

**Methods:**

- `send(topic: str, message: str | bytes, key: str = None, headers: dict = None, message_id: str = None, metadata: dict = None) -> str`: Send a message asynchronously and return the message ID for tracking. Automatically generates message ID if not provided.

- `send_and_wait(topic: str, message: str | bytes, key: str = None, headers: dict = None, message_id: str = None, metadata: dict = None, timeout: float = 30.0) -> str`: Send a message and wait for delivery confirmation. Raises exception if delivery fails or times out.

- `flush(timeout: float = 10.0) -> None`: Wait for all pending messages to be delivered or fail within the specified timeout.

- `poll(timeout: float = 0) -> int`: Poll for delivery events and trigger callbacks. Returns the number of events processed.

- `get_queue_size() -> int`: Get the current number of messages waiting in the producer's internal queue.

- `close() -> None`: Gracefully shutdown the producer with proper resource cleanup.

**Properties:**
- `broker_url` (str): Returns the configured bootstrap servers for backward compatibility
- `settings` (KafkaProducerSettings): Access to the producer settings

**Usage Examples:**

```python
# Asynchronous sending (fastest)
message_id = producer.send("topic", "message")

# Synchronous sending (guaranteed delivery)
message_id = producer.send_and_wait("topic", "message", timeout=30.0)

# Manual callback processing
producer.send("topic", "message")
producer.poll(0.1)  # Process delivery events

# Monitor queue status
if producer.get_queue_size() > 1000:
    print("Producer queue is getting full!")
```


## License
This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.


## Support

- Issues: [GitHub Issues](https://github.com/mtnvencenzo/cezzis-pycore/issues)
- Discussions: [GitHub Discussions](https://github.com/mtnvencenzo/cezzis-pycore/discussions)

## Acknowledgments

Built with:
- [Confluent Kafka Python](https://github.com/confluentinc/confluent-kafka-python) - The underlying Kafka client
- [Poetry](https://python-poetry.org/) - Dependency management and packaging


