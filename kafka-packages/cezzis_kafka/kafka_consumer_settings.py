class KafkaConsumerSettings:
    """Application settings loaded from environment variables and .env files.

    Attributes:
        consumer_id (int): Kafka consumer ID.
        bootstrap_servers (str): Kafka bootstrap servers.
        consumer_group (str): Kafka consumer group ID.
        topic_name (str): Kafka topic name.
        num_consumers (int): Number of Kafka consumer processes to start. Defaults to 1.

    Methods:
        __init__(self, bootstrap_servers: str, consumer_group: str, topic_name: str, num_consumers: int = 1) -> None
    """

    def __init__(
        self,
        consumer_id: int,
        bootstrap_servers: str,
        consumer_group: str,
        topic_name: str,
        num_consumers: int = 1,
    ) -> None:
        """Initialize the KafkaConsumerSettings

        Args:
            consumer_id (int): Kafka consumer ID.
            bootstrap_servers (str): Kafka bootstrap servers.
            consumer_group (str): Kafka consumer group ID.
            topic_name (str): Kafka topic name.
            num_consumers (int): Number of Kafka consumer processes to start. Defaults to 1.
        """

        if consumer_id < 0:
            raise ValueError("Invalid consumer ID")

        if not bootstrap_servers or bootstrap_servers.strip() == "":
            raise ValueError("Bootstrap servers cannot be empty")

        if not consumer_group or consumer_group.strip() == "":
            raise ValueError("Consumer group cannot be empty")

        if not topic_name or topic_name.strip() == "":
            raise ValueError("Topic name cannot be empty")

        if num_consumers < 1:
            raise ValueError("Number of consumers must be at least 1")

        self.consumer_id = consumer_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.topic_name = topic_name
        self.num_consumers = num_consumers
