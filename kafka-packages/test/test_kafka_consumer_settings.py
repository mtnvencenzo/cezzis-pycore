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
            consumer_id=1,
            bootstrap_servers="localhost:9092",
            consumer_group="test-group",
            topic_name="test-topic",
            num_consumers=3,
        )
        assert settings.consumer_id == 1
        assert settings.bootstrap_servers == "localhost:9092"
        assert settings.consumer_group == "test-group"
        assert settings.topic_name == "test-topic"
        assert settings.num_consumers == 3

    def test_uses_defaults_correctly(self) -> None:
        settings = KafkaConsumerSettings(
            consumer_id=1,
            bootstrap_servers="localhost:9092",
            consumer_group="test-group",
            topic_name="test-topic"
        )
        assert settings.consumer_id == 1
        assert settings.bootstrap_servers == "localhost:9092"
        assert settings.consumer_group == "test-group"
        assert settings.topic_name == "test-topic"
        assert settings.num_consumers == 1

    def test_raises_error_on_invalid_consumer_id(self) -> None:
        with pytest.raises(ValueError, match="Invalid consumer ID"):
            KafkaConsumerSettings(
                consumer_id=-1,
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                topic_name="test-topic",
                num_consumers=3,
            )

    def test_raises_error_on_empty_bootstrap_servers(self) -> None:
        with pytest.raises(ValueError, match="Bootstrap servers cannot be empty"):
            KafkaConsumerSettings(
                consumer_id=1,
                bootstrap_servers="   ",
                consumer_group="test-group",
                topic_name="test-topic",
                num_consumers=3,
            )

    def test_raises_error_on_empty_consumer_group(self) -> None:
        with pytest.raises(ValueError, match="Consumer group cannot be empty"):
            KafkaConsumerSettings(
                consumer_id=1,
                bootstrap_servers="localhost:9092",
                consumer_group="",
                topic_name="test-topic",
                num_consumers=3,
            )

    def test_raises_error_on_empty_topic_name(self) -> None:
        with pytest.raises(ValueError, match="Topic name cannot be empty"):
            KafkaConsumerSettings(
                consumer_id=1,
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                topic_name="   ",
                num_consumers=3,
            )

    def test_raises_error_on_invalid_num_consumers(self) -> None:   
        with pytest.raises(ValueError, match="Number of consumers must be at least 1"):
            KafkaConsumerSettings(
                consumer_id=1,
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                topic_name="test-topic",
                num_consumers=0,
            )