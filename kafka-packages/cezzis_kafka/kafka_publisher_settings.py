class KafkaPublisherSettings:
    """Settings for Kafka Publisher.

    Attributes:
        bootstrap_servers (str): Kafka bootstrap servers.

    Methods:
    """

    def __init__(
        self,
        bootstrap_servers: str
    ) -> None:
        """Initialize the KafkaPublisherSettings

        Args:
            bootstrap_servers (str): Kafka bootstrap servers.
        """
        if not bootstrap_servers or bootstrap_servers.strip() == "":
            raise ValueError("Bootstrap servers cannot be empty")

        self.bootstrap_servers = bootstrap_servers
