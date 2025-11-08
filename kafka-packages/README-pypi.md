# Cezzis Kafka

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/cezzis-kafka.svg)](https://pypi.org/project/cezzis-kafka/)
[![CI/CD](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-kafka-cicd.yaml/badge.svg)](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-kafka-cicd.yaml)

A comprehensive, production-ready Python library for Apache Kafka. Provides both high-performance **message consumption** and **reliable message publishing** with enterprise-grade features including automatic retries, dead letter queues, and comprehensive error handling.

## 🚀 Installation

Install `cezzis-kafka` from PyPI:

```bash
pip install cezzis-kafka
```

Or using Poetry:

```bash
poetry add cezzis-kafka
```

## 📋 Requirements

- Python 3.12 or higher
- Apache Kafka cluster (local or remote)

## 📖 Documentation & Examples

**Complete documentation, examples, and guides are available in the GitHub repository:**

**📚 [Full Documentation & Examples](https://github.com/mtnvencenzo/cezzis-pycore/blob/main/kafka-packages/README.md)**

## ✨ Key Features

### 🔽 **Kafka Consumer**
- **Simple Consumer API** - Minimal boilerplate to get started with Kafka consumption
- **Abstract Processor Interface** - Clean separation between Kafka operations and business logic  
- **Multi-Process Support** - Built-in parallel consumer spawning for high throughput
- **Robust Error Handling** - Automatic error detection and lifecycle hooks
- **Structured Logging** - Rich context for debugging and monitoring

### 🔼 **Kafka Producer**
- **High-Performance Publishing** - Optimized for throughput with batching and compression
- **Enterprise Retry Mechanism** - Automatic retries with exponential backoff using threading.Timer
- **Dead Letter Queue (DLQ)** - Automatic routing of failed messages after retry exhaustion
- **Error Classification** - Intelligent handling of retriable vs. terminal errors
- **Delivery Tracking** - Comprehensive delivery callbacks and status monitoring
- **Thread-Safe** - Safe for concurrent use in multi-threaded applications

### 🏢 **Enterprise Features**
- **Configuration Management** - Settings classes for both consumer and producer
- **Graceful Shutdown** - Proper resource cleanup and message delivery completion
- **Metrics Integration** - Extensible metrics and monitoring support
- **Type Safety** - Full type hints for better IDE support and code quality

## 🏁 Quick Start

### Consumer Example

```python
from cezzis_kafka import IKafkaMessageProcessor, KafkaConsumerSettings, start_consumer
from confluent_kafka import Message
from multiprocessing import Event

class SimpleProcessor(IKafkaMessageProcessor):
    def __init__(self, settings: KafkaConsumerSettings):
        self._settings = settings
    
    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "SimpleProcessor":
        return SimpleProcessor(kafka_settings)
    
    def kafka_settings(self) -> KafkaConsumerSettings:
        return self._settings
    
    def message_received(self, msg: Message) -> None:
        """Process received messages - implement your business logic here."""
        value = msg.value().decode('utf-8')
        print(f"Received: {value}")
    
    # ... other required lifecycle methods

# Configure and start
settings = KafkaConsumerSettings(
    consumer_id=1,
    bootstrap_servers="localhost:9092",
    consumer_group="my-group",
    topic_name="messages"
)

processor = SimpleProcessor.CreateNew(settings)
start_consumer(Event(), processor)
```

### Producer Example

```python
from cezzis_kafka import KafkaPublisher, KafkaPublisherSettings
import json

# Configure producer with retry and DLQ
settings = KafkaPublisherSettings(
    bootstrap_servers="localhost:9092",
    topic_name="orders",
    enable_retry=True,
    max_retry_attempts=3,
    dlq_topic_name="orders-dlq"
)

publisher = KafkaPublisher(settings)

# Send message with automatic retry/DLQ handling
message = json.dumps({"order_id": "12345", "amount": 99.99})
future = publisher.send(key="order-12345", value=message)

# Wait for delivery confirmation
result = future.get(timeout=30)
print(f"Delivered to partition {result.partition}, offset {result.offset}")

publisher.close()  # Graceful shutdown
```

## 📚 Comprehensive Examples

For detailed examples, advanced patterns, and production configurations, see the **[complete documentation](https://github.com/mtnvencenzo/cezzis-pycore/blob/main/kafka-packages/README.md)** which includes:


## 📄 License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/mtnvencenzo/cezzis-pycore/blob/main/LICENSE) file for details.

---

**Happy streaming! 🚀**
