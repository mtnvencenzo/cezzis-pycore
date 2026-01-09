import logging
from multiprocessing import Process
from multiprocessing.synchronize import Event as EventType
from typing import Type, TypeVar

from confluent_kafka import Consumer, KafkaError

# Application specific imports
from cezzis_kafka.ikafka_message_processor import IKafkaMessageProcessor
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings

logger = logging.getLogger(__name__)


def start_consumer(stop_event: EventType, processor: IKafkaMessageProcessor) -> None:
    """Start the Kafka consumer and begin polling for messages.

    Args:
        stop_event (EventType): An event to signal when to stop the consumer.
        processor (IKafkaMessageProcessor): An instance of IKafkaMessageProcessor to handle message processing.
    """
    consumer = _create_consumer(processor)

    if consumer is None:
        return

    try:
        _subscribe_consumer(consumer, processor)

        _start_polling(stop_event, consumer, processor)

    except KeyboardInterrupt as i:
        logger.info(
            "Keyboard interrupt received, shutting down consumer",
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                "process.interrupt": str(i),
            },
        )
    except Exception as e:
        logger.error(
            "Unexpected error in consumer, shutting down consumer",
            exc_info=True,
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                "error": str(e),
            },
        )
    finally:
        _close_consumer(consumer, processor)


TProcessor = TypeVar("TProcessor", bound=IKafkaMessageProcessor)
"""Type variable for IKafkaMessageProcessor or its subclasses."""


def spawn_consumers(
    factory_type: Type[TProcessor],
    num_consumers: int,
    stop_event: EventType,
    bootstrap_servers: str,
    consumer_group: str,
    topic_name: str,
) -> None:
    """Spawn multiple Kafka consumers under a single consumer group.

    Args:
        factory_type (Type[TProcessor]): The factory type to create IKafkaMessageProcessor instances.
        num_consumers (int): The number of consumers to spawn.
        stop_event (EventType): An event to signal when to stop the consumer.
        bootstrap_servers (str): The Kafka bootstrap servers.
        consumer_group (str): The consumer group ID.
        topic_name (str): The topic name to subscribe to.
    """

    logger.info(
        "Spawning Kafka consumers",
        extra={
            "messaging.kafka.num_consumers": num_consumers,
            "messaging.kafka.bootstrap_servers": bootstrap_servers,
            "messaging.kafka.consumer_group": consumer_group,
            "messaging.kafka.topic_name": topic_name,
        },
    )

    processes: list[Process] = []

    for i in range(num_consumers):
        processor = factory_type.CreateNew(
            kafka_settings=KafkaConsumerSettings(
                bootstrap_servers=bootstrap_servers,
                consumer_group=consumer_group,
                topic_name=topic_name,
                num_consumers=num_consumers,
            )
        )

        p = Process(
            target=start_consumer,
            args=(
                stop_event,
                processor,
            ),
        )

        processes.append(p)
        p.start()
        logger.info(
            "Started Kafka consumer process",
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                "process.pid": p.pid,
            },
        )

    for i, p in enumerate(processes):
        p.join()  # waiting for all to complete
        logger.info(
            "Kafka consumer process completed",
            extra={
                "process.pid": p.pid,
                "process.exitcode": p.exitcode,
                "process.name": p.name,
            },
        )


def _create_consumer(processor: IKafkaMessageProcessor) -> Consumer | None:
    """Create and start a Kafka consumer.

    Args:
        processor (IKafkaMessageProcessor): The Kafka processor.

    Returns:
        Consumer | None: The created Kafka consumer or None.
    """
    global logger

    logger.info(
        "Creating Kafka consumer",
        extra={
            "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
            "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
            "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
        },
    )

    try:
        # Call the consumer creating hook
        processor.consumer_creating()

        consumer = Consumer(
            {
                "bootstrap.servers": processor.kafka_settings().bootstrap_servers,
                "group.id": processor.kafka_settings().consumer_group,
                "auto.offset.reset": "earliest",
            }
        )

        # Call the consumer created hook
        processor.consumer_created(consumer)

        logger.info(
            "Kafka consumer created successfully",
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
            },
        )

        return consumer

    except Exception as e:
        logger.error(
            f"Failed to create Kafka consumer. Exiting {str(e)}",
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                "error": str(e),
            },
        )

        return None


def _subscribe_consumer(consumer: Consumer, processor: IKafkaMessageProcessor) -> None:
    """Subscribe the Kafka consumer to the specified topic.

    Args:
        consumer (Consumer): The Kafka consumer.
        processor (IKafkaMessageProcessor): The message processor for handling messages.

    Returns:
        None
    """
    global logger

    logger.info(
        "Subscribing consumer to topic",
        extra={
            "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
            "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
            "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
        },
    )

    try:
        consumer.subscribe([processor.kafka_settings().topic_name])

        logger.info(
            "Consumer subscribed successfully",
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
            },
        )

        processor.consumer_subscribed()

    except Exception as e:
        logger.error(
            "Error subscribing consumer to Kafka topic",
            exc_info=True,
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                "error": str(e),
            },
        )
        raise


def _start_polling(stop_event: EventType, consumer: Consumer, processor: IKafkaMessageProcessor) -> None:
    """Start polling for messages from the Kafka consumer.

    Args:
        stop_event (EventType): An event to signal when to stop the consumer.
        consumer (Consumer): The Kafka consumer.
        processor (IKafkaMessageProcessor): The message processor for handling messages.
    """
    global logger

    logger.info(
        "Start consumer polling for messages...",
        extra={
            "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
            "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
            "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
        },
    )

    # Loop until the app is being shutdown
    while stop_event.is_set() is False:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        elif msg.error():
            error = msg.error()
            if error is not None and error.code() == KafkaError._PARTITION_EOF:
                logger.info(
                    "End of partition reached",
                    extra={
                        "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                        "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                        "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                        "messaging.kafka.partition": msg.partition(),
                    },
                )

                processor.message_partition_reached(msg)

            else:
                logger.error(
                    "Consumer message error",
                    exc_info=True,
                    extra={
                        "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                        "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                        "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                        "messaging.kafka.partition": msg.partition(),
                        "messaging.kafka.error": str(error),
                    },
                )

                processor.message_error_received(msg)

            continue
        else:
            processor.message_received(msg)


def _close_consumer(consumer: Consumer, processor: IKafkaMessageProcessor) -> None:
    """Cleanup function to close the Kafka consumer.

    Args:
        consumer (Consumer): The Kafka consumer.
        processor (IKafkaMessageProcessor): The Kafka processor.
    """
    global logger

    logger.info(
        "Shutting down kafka consumer",
        extra={
            "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
            "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
            "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
        },
    )

    processor.consumer_stopping()

    try:
        logger.info(
            "Committing offsets for kafka consumer",
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
            },
        )
        consumer.commit()
    except Exception as e:
        logger.error(
            "Error committing offsets for kafka consumer",
            extra={
                "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
                "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
                "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
                "error": str(e),
            },
        )

    logger.info(
        "Closing kafka consumer",
        extra={
            "messaging.kafka.bootstrap_servers": processor.kafka_settings().bootstrap_servers,
            "messaging.kafka.consumer_group": processor.kafka_settings().consumer_group,
            "messaging.kafka.topic_name": processor.kafka_settings().topic_name,
        },
    )

    consumer.close()
