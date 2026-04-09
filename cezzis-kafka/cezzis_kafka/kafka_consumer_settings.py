class KafkaConsumerSettings:
    """Kafka Consumer Settings.

    Attributes:
        bootstrap_servers (str): Kafka bootstrap servers.
        consumer_group (str): Kafka consumer group ID.
        topic_name (str): Kafka topic name.
        num_consumers (int): Number of Kafka consumer processes to start. Defaults to 1.
        max_poll_interval_ms (int): Maximum poll interval in milliseconds. Defaults to 300000.
        auto_offset_reset (str): Auto offset reset policy. Defaults to "earliest".  Accepted values are "earliest", "latest", and "none".
        reconnect_backoff_seconds (float): Initial backoff in seconds between reconnect attempts. Defaults to 5.0.
        reconnect_backoff_max_seconds (float): Maximum backoff in seconds between reconnect attempts. Defaults to 120.0.
        reconnect_max_retries (int): Maximum number of reconnect attempts. 0 means infinite retries. Defaults to 0.

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
        reconnect_backoff_seconds: float = 5.0,
        reconnect_backoff_max_seconds: float = 120.0,
        reconnect_max_retries: int = 0,
    ) -> None:
        """Initialize the KafkaConsumerSettings

        Args:
            bootstrap_servers (str): Kafka bootstrap servers.
            consumer_group (str): Kafka consumer group ID.
            topic_name (str): Kafka topic name.
            num_consumers (int): Number of Kafka consumer processes to start. Defaults to 1.
            max_poll_interval_ms (int): Maximum poll interval in milliseconds. Defaults to 300000.
            auto_offset_reset (str): Auto offset reset policy. Defaults to "earliest".  Accepted values are "earliest", "latest", and "none".
            reconnect_backoff_seconds (float): Initial backoff in seconds between reconnect attempts. Defaults to 5.0.
            reconnect_backoff_max_seconds (float): Maximum backoff in seconds between reconnect attempts. Defaults to 120.0.
            reconnect_max_retries (int): Maximum number of reconnect attempts. 0 means infinite retries. Defaults to 0.
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

        if reconnect_backoff_seconds <= 0:
            raise ValueError("Reconnect backoff seconds must be greater than 0")

        if reconnect_backoff_max_seconds < reconnect_backoff_seconds:
            raise ValueError("Reconnect backoff max seconds must be greater than or equal to reconnect backoff seconds")

        if reconnect_max_retries < 0:
            raise ValueError("Reconnect max retries must be greater than or equal to 0")

        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.topic_name = topic_name
        self.num_consumers = num_consumers
        self.max_poll_interval_ms = max_poll_interval_ms
        self.auto_offset_reset = auto_offset_reset
        self.reconnect_backoff_seconds = reconnect_backoff_seconds
        self.reconnect_backoff_max_seconds = reconnect_backoff_max_seconds
        self.reconnect_max_retries = reconnect_max_retries
