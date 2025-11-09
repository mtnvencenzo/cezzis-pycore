from typing import Any, Callable, Dict, Optional


class KafkaPublisherSettings:
    """Settings for Kafka Publisher.

    Attributes:
        bootstrap_servers (str): Kafka bootstrap servers.
        max_retries (int): Maximum number of retries for retriable errors.
        metrics_callback (Optional[Callable[[str, Dict[str, Any]], None]]): Callback for reporting metrics.
        producer_config (Optional[Dict[str, Any]]): Additional producer configuration.

    Methods:
    """

    def __init__(
        self,
        bootstrap_servers: str,
        max_retries: int = 3,
        metrics_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        producer_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the KafkaPublisherSettings

        Args:
            bootstrap_servers (str): Kafka bootstrap servers.
            max_retries (int): Maximum number of retries for retriable errors. Defaults to 3.
            metrics_callback (Optional[Callable[[str, Dict[str, Any]], None]]): Callback for reporting metrics.
            producer_config (Optional[Dict[str, Any]]): Additional producer configuration to override defaults.

        Raises:
            ValueError: If bootstrap_servers is empty or invalid.
            ValueError: If max_retries is negative.
        """
        if not bootstrap_servers or bootstrap_servers.strip() == "":
            raise ValueError("Bootstrap servers cannot be empty")

        if max_retries < 0:
            raise ValueError("Max retries cannot be negative")

        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.metrics_callback = metrics_callback
        self.producer_config = (producer_config or {}).copy()  # Make a copy to avoid mutation
