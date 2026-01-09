from typing import Any, Callable, Dict, Optional

from confluent_kafka import KafkaError, Message


class KafkaProducerSettings:
    """Settings for Kafka Producer.

    Attributes:
        bootstrap_servers (str): Kafka bootstrap servers.
        max_retries (int): Maximum number of retries for retriable errors. Must be >= 1
        retry_backoff_ms (int): Initial backoff time in milliseconds between retries.
        retry_backoff_max_ms (int): Maximum backoff time in milliseconds between retries.
        delivery_timeout_ms (int): Total timeout for message delivery including retries.
        request_timeout_ms (int): Timeout for individual produce requests.
        on_delivery (Optional[Callable[[KafkaError | None, Message], None]]): Callback for delivery reports.
        producer_config (Optional[Dict[str, Any]]): Additional producer configuration.

    Methods:
    """

    def __init__(
        self,
        bootstrap_servers: str,
        max_retries: int = 3,
        retry_backoff_ms: int = 100,
        retry_backoff_max_ms: int = 1000,
        delivery_timeout_ms: int = 300000,
        request_timeout_ms: int = 30000,
        on_delivery: Optional[Callable[[KafkaError | None, Message], None]] = None,
        producer_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the KafkaPublisherSettings

        Args:
            bootstrap_servers (str): Kafka bootstrap servers.
            max_retries (int): Maximum number of retries for retriable errors. Defaults to 3 and must be >= 1.
            retry_backoff_ms (int): Initial backoff time in milliseconds between retries. Defaults to 100.
            retry_backoff_max_ms (int): Maximum backoff time in milliseconds between retries. Defaults to 1000.
            delivery_timeout_ms (int): Total timeout for message delivery including retries. Defaults to 300000 (5 minutes).
            request_timeout_ms (int): Timeout for individual produce requests. Defaults to 30000 (30 seconds).
            on_delivery (Optional[Callable[[KafkaError | None, Message], None]]): Callback for delivery reports.
            producer_config (Optional[Dict[str, Any]]): Additional producer configuration to override defaults.

        Raises:
            ValueError: If bootstrap_servers is empty or invalid.
            ValueError: If max_retries is less than 1 (required for idempotent producer).
            ValueError: If timeout values are invalid.
        """
        if not bootstrap_servers or bootstrap_servers.strip() == "":
            raise ValueError("Bootstrap servers cannot be empty")

        if max_retries < 1:
            raise ValueError("Max retries must be at least 1 to support idempotent producer configuration")

        if retry_backoff_ms < 1 or retry_backoff_ms > 300000:
            raise ValueError("Retry backoff must be between 1 and 300000 milliseconds")

        if retry_backoff_max_ms < retry_backoff_ms or retry_backoff_max_ms > 300000:
            raise ValueError("Max retry backoff must be >= retry_backoff_ms and <= 300000 milliseconds")

        if delivery_timeout_ms < 1000:
            raise ValueError("Delivery timeout must be at least 1000 milliseconds")

        if request_timeout_ms < 1000:
            raise ValueError("Request timeout must be at least 1000 milliseconds")

        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.retry_backoff_max_ms = retry_backoff_max_ms
        self.delivery_timeout_ms = delivery_timeout_ms
        self.request_timeout_ms = request_timeout_ms
        self.producer_config = (producer_config or {}).copy()  # Make a copy to avoid mutation
        self.on_delivery = on_delivery
