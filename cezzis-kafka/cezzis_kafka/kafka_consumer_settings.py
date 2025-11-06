class KafkaConsumerSettings:
    """Application settings loaded from environment variables and .env files.

    Attributes:
        consumer_id (int): Kafka consumer ID.
        bootstrap_servers (str): Kafka bootstrap servers.
        consumer_group (str): Kafka consumer group ID.
        topic_name (str): Kafka topic name.
        num_consumers (int): Number of Kafka consumer processes to start.

    Methods:
        __init__(self, bootstrap_servers: str, consumer_group: str, topic_name: str, num_consumers: int) -> None
    """

    consumer_id: int
    bootstrap_servers: str
    consumer_group: str
    topic_name: str
    num_consumers: int

    def __init__(
        self,
        consumer_id: int,
        bootstrap_servers: str,
        consumer_group: str,
        topic_name: str,
        num_consumers: int,
    ) -> None:
        """Initialize the KafkaConsumerSettings

        Args:
            consumer_id (int): Kafka consumer ID.
            bootstrap_servers (str): Kafka bootstrap servers.
            consumer_group (str): Kafka consumer group ID.
            topic_name (str): Kafka topic name.
            num_consumers (int): Number of Kafka consumer processes to start.
        """
        self.consumer_id = consumer_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group = consumer_group
        self.topic_name = topic_name
        self.num_consumers = num_consumers
