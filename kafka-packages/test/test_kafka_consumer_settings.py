"""Unit tests for kafka_consumer_settings module."""

import pytest

from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings


class TestKafkaConsumerSettings:
    """Unit tests for kafka_consumer_settings module.

    This test class contains tests to verify that the KafkaConsumerSettings class
    correctly assigns configuration values from the provided parameters.
    """

    def test_assigns_setting_values_correctly(self) -> None:
        settings = KafkaConsumerSettings(
            bootstrap_servers="localhost:9092",
            consumer_group="test-group",
            topic_name="test-topic",
            num_consumers=3,
        )
        assert settings.bootstrap_servers == "localhost:9092"
        assert settings.consumer_group == "test-group"
        assert settings.topic_name == "test-topic"
        assert settings.num_consumers == 3

    def test_uses_defaults_correctly(self) -> None:
        settings = KafkaConsumerSettings(
            bootstrap_servers="localhost:9092", consumer_group="test-group", topic_name="test-topic"
        )
        assert settings.bootstrap_servers == "localhost:9092"
        assert settings.consumer_group == "test-group"
        assert settings.topic_name == "test-topic"
        assert settings.num_consumers == 1

    def test_raises_error_on_empty_bootstrap_servers(self) -> None:
        with pytest.raises(ValueError, match="Bootstrap servers cannot be empty"):
            KafkaConsumerSettings(
                bootstrap_servers="   ",
                consumer_group="test-group",
                topic_name="test-topic",
                num_consumers=3,
            )

    def test_raises_error_on_empty_consumer_group(self) -> None:
        with pytest.raises(ValueError, match="Consumer group cannot be empty"):
            KafkaConsumerSettings(
                bootstrap_servers="localhost:9092",
                consumer_group="",
                topic_name="test-topic",
                num_consumers=3,
            )

    def test_raises_error_on_empty_topic_name(self) -> None:
        with pytest.raises(ValueError, match="Topic name cannot be empty"):
            KafkaConsumerSettings(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                topic_name="   ",
                num_consumers=3,
            )

    def test_raises_error_on_invalid_num_consumers(self) -> None:
        with pytest.raises(ValueError, match="Number of consumers must be at least 1"):
            KafkaConsumerSettings(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                topic_name="test-topic",
                num_consumers=0,
            )

    @pytest.mark.parametrize("auto_offset_reset", ["earliest", "latest", "none"])
    def test_auto_offset_reset_accepts_available_values(self, auto_offset_reset: str):
        """Test that auto_offset_reset values allows acceptable values."""
        KafkaConsumerSettings(
            bootstrap_servers="localhost:9092",
            consumer_group="test-group",
            topic_name="test-topic",
            num_consumers=1,
            auto_offset_reset=auto_offset_reset,
        )

    @pytest.mark.parametrize("auto_offset_reset", ["earlier", "", ""])
    def test_invalid_auto_offset_reset_raises_error(self, auto_offset_reset: str):
        """Test that invalid auto_offset_reset values raise ValueError."""
        with pytest.raises(ValueError, match="Invalid auto offset reset value"):
            KafkaConsumerSettings(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                topic_name="test-topic",
                num_consumers=1,
                auto_offset_reset=auto_offset_reset,
            )

    @pytest.mark.parametrize("max_poll_interval_ms", [0, -1])
    def test_invalid_max_poll_interval_ms_raises_error(self, max_poll_interval_ms: int):
        """Test that invalid max_poll_interval_ms values raise ValueError."""
        with pytest.raises(ValueError, match="Max poll interval must be at least 1 ms"):
            KafkaConsumerSettings(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                topic_name="test-topic",
                num_consumers=1,
                max_poll_interval_ms=max_poll_interval_ms,
            )
