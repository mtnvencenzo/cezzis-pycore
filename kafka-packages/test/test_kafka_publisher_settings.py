"""
Tests for KafkaPublisherSettings class.
"""

import pytest

from cezzis_kafka.kafka_publisher_settings import KafkaPublisherSettings


class TestKafkaPublisherSettings:
    """Test suite for KafkaPublisherSettings class."""

    def test_init_with_required_parameters(self):
        """Test initialization with only required parameters."""
        settings = KafkaPublisherSettings(bootstrap_servers="localhost:9092")

        assert settings.bootstrap_servers == "localhost:9092"
        assert settings.max_retries == 3  # Default value
        assert settings.metrics_callback is None
        assert settings.producer_config == {}

    def test_init_with_all_parameters(self):
        """Test initialization with all parameters."""

        def mock_metrics_callback(metric_name, metric_data):
            pass

        producer_config = {"batch.size": 16384, "linger.ms": 10}

        settings = KafkaPublisherSettings(
            bootstrap_servers="kafka1:9092,kafka2:9092",
            max_retries=5,
            metrics_callback=mock_metrics_callback,
            producer_config=producer_config,
        )

        assert settings.bootstrap_servers == "kafka1:9092,kafka2:9092"
        assert settings.max_retries == 5
        assert settings.metrics_callback == mock_metrics_callback
        assert settings.producer_config == producer_config

    def test_init_with_custom_max_retries(self):
        """Test initialization with custom max_retries."""
        settings = KafkaPublisherSettings(bootstrap_servers="localhost:9092", max_retries=0)

        assert settings.max_retries == 0

    def test_init_with_none_producer_config(self):
        """Test initialization with None producer_config defaults to empty dict."""
        settings = KafkaPublisherSettings(bootstrap_servers="localhost:9092", producer_config=None)

        assert settings.producer_config == {}

    def test_empty_bootstrap_servers_raises_error(self):
        """Test that empty bootstrap servers raise ValueError."""
        with pytest.raises(ValueError, match="Bootstrap servers cannot be empty"):
            KafkaPublisherSettings(bootstrap_servers="")

    def test_whitespace_only_bootstrap_servers_raises_error(self):
        """Test that whitespace-only bootstrap servers raise ValueError."""
        with pytest.raises(ValueError, match="Bootstrap servers cannot be empty"):
            KafkaPublisherSettings(bootstrap_servers="   ")

    def test_negative_max_retries_raises_error(self):
        """Test that negative max_retries raises ValueError."""
        with pytest.raises(ValueError, match="Max retries cannot be negative"):
            KafkaPublisherSettings(bootstrap_servers="localhost:9092", max_retries=-1)

    def test_producer_config_is_copied(self):
        """Test that producer_config is properly handled as a copy."""
        original_config = {"batch.size": 1024}

        settings = KafkaPublisherSettings(bootstrap_servers="localhost:9092", producer_config=original_config)

        # Modify original config
        original_config["linger.ms"] = 5

        # Settings should not be affected
        assert "linger.ms" not in settings.producer_config
        assert settings.producer_config == {"batch.size": 1024}

    @pytest.mark.parametrize(
        "servers",
        [
            "localhost:9092",
            "kafka1:9092,kafka2:9092,kafka3:9092",
            "192.168.1.100:9092",
            "my-kafka-cluster.example.com:9092",
        ],
    )
    def test_valid_bootstrap_servers(self, servers):
        """Test various valid bootstrap server formats."""
        settings = KafkaPublisherSettings(bootstrap_servers=servers)
        assert settings.bootstrap_servers == servers

    @pytest.mark.parametrize("retries", [0, 1, 3, 5, 10, 100])
    def test_valid_max_retries(self, retries):
        """Test various valid max_retries values."""
        settings = KafkaPublisherSettings(bootstrap_servers="localhost:9092", max_retries=retries)
        assert settings.max_retries == retries

    def test_settings_immutability_concept(self):
        """Test that settings can be used to create different configurations."""
        base_config = {"bootstrap_servers": "localhost:9092", "max_retries": 2}

        # Development settings
        dev_settings = KafkaPublisherSettings(**base_config)

        # Production settings with DLQ
        prod_settings = KafkaPublisherSettings(**base_config, producer_config={"acks": "all", "retries": 5})

        assert dev_settings.producer_config == {}
        assert prod_settings.producer_config == {"acks": "all", "retries": 5}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
