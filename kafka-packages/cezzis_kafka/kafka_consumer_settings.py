class KafkaConsumerSettings:
    """Kafka Consumer Settings.

    Attributes:
        bootstrap_servers (str): Kafka bootstrap servers.
        consumer_group (str): Kafka consumer group ID.
        topic_name (str): Kafka topic name.
        num_consumers (int): Number of Kafka consumer processes to start. Defaults to 1.
        max_poll_interval_ms (int): Maximum poll interval in milliseconds. Defaults to 300000.
        auto_offset_reset (str): Auto offset reset policy. Defaults to "earliest".  Accepted values are "earliest", "latest", and "none".

    Methods:
        __init__(self, bootstrap_servers: str, consumer_group: str, topic_name: str, num_consumers: int = 1) -> None
    """

    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        topic_name: str,
        num_consumers: int = 1,
        max_poll_interval_ms: int = 300000,
        auto_offset_reset: str = "earliest",
    ) -> None:
        """Initialize the KafkaConsumerSettings

        Args:
            bootstrap_servers (str): Kafka bootstrap servers.
            consumer_group (str): Kafka consumer group ID.
            topic_name (str): Kafka topic name.
            num_consumers (int): Number of Kafka consumer processes to start. Defaults to 1.
            max_poll_interval_ms (int): Maximum poll interval in milliseconds. Defaults to 300000.
            auto_offset_reset (str): Auto offset reset policy. Defaults to "earliest".  Accepted values are "earliest", "latest", and "none".
        """
        if not bootstrap_servers or bootstrap_servers.strip() == "":
            raise ValueError("Bootstrap servers cannot be empty")

        if not consumer_group or consumer_group.strip() == "":
            raise ValueError("Consumer group cannot be empty")

        if not topic_name or topic_name.strip() == "":
            raise ValueError("Topic name cannot be empty")

        if num_consumers < 1:
            raise ValueError("Number of consumers must be at least 1")

        if max_poll_interval_ms < 1:
            raise ValueError("Max poll interval must be at least 1 ms")

        if auto_offset_reset not in ["earliest", "latest", "none"]:
            raise ValueError("Invalid auto offset reset value")

        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.topic_name = topic_name
        self.num_consumers = num_consumers
        self.max_poll_interval_ms = max_poll_interval_ms
        self.auto_offset_reset = auto_offset_reset
