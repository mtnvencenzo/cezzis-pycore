"""Unit tests for async_kafka_consumer module — reconnection and retry logic."""

import asyncio
import json
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from confluent_kafka import Consumer, KafkaException, Message

from cezzis_kafka.async_kafka_consumer import (
    _calculate_backoff,
    _ConsumerCreationError,
    _create_consumer_async,
    start_consumer_async,
)
from cezzis_kafka.iasync_kafka_message_processor import IAsyncKafkaMessageProcessor
from cezzis_kafka.kafka_consumer_settings import KafkaConsumerSettings


class MockAsyncProcessor(IAsyncKafkaMessageProcessor):
    """Mock implementation of IAsyncKafkaMessageProcessor for testing."""

    def __init__(self, settings: KafkaConsumerSettings):
        self._settings = settings
        self.consumer_creating_count = 0
        self.consumer_created_count = 0
        self.consumer_subscribed_count = 0
        self.consumer_stopping_count = 0
        self.messages_received: list[Message] = []

    @staticmethod
    def CreateNew(kafka_settings: KafkaConsumerSettings) -> "MockAsyncProcessor":
        return MockAsyncProcessor(kafka_settings)

    def kafka_settings(self) -> KafkaConsumerSettings:
        return self._settings

    async def consumer_creating(self) -> None:
        self.consumer_creating_count += 1

    async def consumer_created(self, consumer: Optional[Consumer]) -> None:
        self.consumer_created_count += 1

    async def consumer_subscribed(self) -> None:
        self.consumer_subscribed_count += 1

    async def consumer_stopping(self) -> None:
        self.consumer_stopping_count += 1

    async def message_received(self, msg: Message) -> None:
        self.messages_received.append(msg)

    async def message_error_received(self, msg: Message) -> None:
        pass

    async def message_partition_reached(self, msg: Message) -> None:
        pass


@pytest.fixture
def settings() -> KafkaConsumerSettings:
    return KafkaConsumerSettings(
        bootstrap_servers="localhost:9092",
        consumer_group="test-group",
        topic_name="test-topic",
        reconnect_backoff_seconds=0.01,
        reconnect_backoff_max_seconds=0.05,
        reconnect_max_retries=0,
    )


@pytest.fixture
def processor(settings: KafkaConsumerSettings) -> MockAsyncProcessor:
    return MockAsyncProcessor(settings)


class TestCalculateBackoff:
    """Tests for _calculate_backoff function."""

    def test_first_attempt_uses_base(self) -> None:
        """First attempt backoff should be in [base/2, base]."""
        for _ in range(50):
            val = _calculate_backoff(attempt=1, base_seconds=10.0, max_seconds=120.0)
            assert 5.0 <= val <= 10.0

    def test_second_attempt_doubles(self) -> None:
        """Second attempt should be in [base, 2*base]."""
        for _ in range(50):
            val = _calculate_backoff(attempt=2, base_seconds=10.0, max_seconds=120.0)
            assert 10.0 <= val <= 20.0

    def test_respects_max_cap(self) -> None:
        """Very high attempt should be capped at max_seconds."""
        for _ in range(50):
            val = _calculate_backoff(attempt=100, base_seconds=5.0, max_seconds=60.0)
            assert val <= 60.0

    def test_backoff_always_positive(self) -> None:
        """Backoff should always be positive."""
        val = _calculate_backoff(attempt=1, base_seconds=0.1, max_seconds=1.0)
        assert val > 0


class TestStartConsumerAsyncRetry:
    """Tests for start_consumer_async reconnection logic."""

    @pytest.mark.asyncio
    async def test_retries_on_consumer_creation_failure(self, processor: MockAsyncProcessor) -> None:
        """When consumer creation fails, it should retry until max_retries is reached."""
        processor._settings.reconnect_max_retries = 3

        with patch("cezzis_kafka.async_kafka_consumer._create_consumer_async", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = None  # Always fail to create

            with pytest.raises(_ConsumerCreationError):
                await start_consumer_async(processor)

            # Should have attempted creation 1 (initial) + 3 (retries that hit the max) = at least 3 calls
            assert mock_create.call_count >= 3

    @pytest.mark.asyncio
    async def test_retries_on_runtime_exception(self, processor: MockAsyncProcessor) -> None:
        """When polling raises an exception, it should retry."""
        processor._settings.reconnect_max_retries = 2

        mock_consumer = MagicMock(spec=Consumer)
        mock_consumer.commit = MagicMock()
        mock_consumer.close = MagicMock()

        with (
            patch("cezzis_kafka.async_kafka_consumer._create_consumer_async", new_callable=AsyncMock) as mock_create,
            patch("cezzis_kafka.async_kafka_consumer._subscribe_consumer_async", new_callable=AsyncMock),
            patch(
                "cezzis_kafka.async_kafka_consumer._start_polling_async",
                new_callable=AsyncMock,
                side_effect=KafkaException(MagicMock()),
            ),
            patch("cezzis_kafka.async_kafka_consumer._close_consumer_async", new_callable=AsyncMock),
        ):
            mock_create.return_value = mock_consumer

            with pytest.raises(KafkaException):
                await start_consumer_async(processor)

            # Should have retried: initial attempt + reconnect_max_retries attempts
            assert mock_create.call_count >= 2

    @pytest.mark.asyncio
    async def test_cancelled_error_not_retried(self, processor: MockAsyncProcessor) -> None:
        """asyncio.CancelledError should propagate immediately without retry."""
        processor._settings.reconnect_max_retries = 5

        mock_consumer = MagicMock(spec=Consumer)
        mock_consumer.commit = MagicMock()
        mock_consumer.close = MagicMock()

        with (
            patch("cezzis_kafka.async_kafka_consumer._create_consumer_async", new_callable=AsyncMock) as mock_create,
            patch("cezzis_kafka.async_kafka_consumer._subscribe_consumer_async", new_callable=AsyncMock),
            patch(
                "cezzis_kafka.async_kafka_consumer._start_polling_async",
                new_callable=AsyncMock,
                side_effect=asyncio.CancelledError(),
            ),
            patch("cezzis_kafka.async_kafka_consumer._close_consumer_async", new_callable=AsyncMock),
        ):
            mock_create.return_value = mock_consumer

            with pytest.raises(asyncio.CancelledError):
                await start_consumer_async(processor)

            # Should have only tried once — no retries on cancellation
            assert mock_create.call_count == 1

    @pytest.mark.asyncio
    async def test_infinite_retries_when_max_is_zero(self, processor: MockAsyncProcessor) -> None:
        """When reconnect_max_retries is 0, should retry indefinitely (we stop via side_effect)."""
        processor._settings.reconnect_max_retries = 0

        call_count = 0

        async def create_side_effect(proc):
            nonlocal call_count
            call_count += 1
            if call_count >= 5:
                # After 5 attempts, cancel to break the infinite loop
                raise asyncio.CancelledError()
            return None  # Fail creation

        with patch(
            "cezzis_kafka.async_kafka_consumer._create_consumer_async",
            side_effect=create_side_effect,
        ):
            with pytest.raises(asyncio.CancelledError):
                await start_consumer_async(processor)

            assert call_count == 5

    @pytest.mark.asyncio
    async def test_consumer_closed_on_runtime_error(self, processor: MockAsyncProcessor) -> None:
        """Consumer should be closed even when polling raises."""
        processor._settings.reconnect_max_retries = 1

        mock_consumer = MagicMock(spec=Consumer)
        mock_consumer.commit = MagicMock()
        mock_consumer.close = MagicMock()

        with (
            patch("cezzis_kafka.async_kafka_consumer._create_consumer_async", new_callable=AsyncMock) as mock_create,
            patch("cezzis_kafka.async_kafka_consumer._subscribe_consumer_async", new_callable=AsyncMock),
            patch(
                "cezzis_kafka.async_kafka_consumer._start_polling_async",
                new_callable=AsyncMock,
                side_effect=RuntimeError("broker down"),
            ),
            patch("cezzis_kafka.async_kafka_consumer._close_consumer_async", new_callable=AsyncMock) as mock_close,
        ):
            mock_create.return_value = mock_consumer

            with pytest.raises(RuntimeError, match="broker down"):
                await start_consumer_async(processor)

            # Close should have been called for each attempt
            assert mock_close.call_count >= 1

    @pytest.mark.asyncio
    async def test_successful_connection_after_failures(self, processor: MockAsyncProcessor) -> None:
        """Consumer should successfully connect after initial failures."""
        processor._settings.reconnect_max_retries = 0  # infinite

        mock_consumer = MagicMock(spec=Consumer)
        mock_consumer.commit = MagicMock()
        mock_consumer.close = MagicMock()

        create_count = 0

        async def create_side_effect(proc):
            nonlocal create_count
            create_count += 1
            if create_count <= 2:
                return None  # Fail first 2 attempts
            return mock_consumer  # Succeed on 3rd

        poll_called = False

        async def polling_side_effect(consumer, proc):
            nonlocal poll_called
            poll_called = True
            raise asyncio.CancelledError()  # Stop after successful connect

        with (
            patch("cezzis_kafka.async_kafka_consumer._create_consumer_async", side_effect=create_side_effect),
            patch("cezzis_kafka.async_kafka_consumer._subscribe_consumer_async", new_callable=AsyncMock),
            patch("cezzis_kafka.async_kafka_consumer._start_polling_async", side_effect=polling_side_effect),
            patch("cezzis_kafka.async_kafka_consumer._close_consumer_async", new_callable=AsyncMock),
        ):
            with pytest.raises(asyncio.CancelledError):
                await start_consumer_async(processor)

            assert create_count == 3
            assert poll_called


class TestStartPollingKafkaException:
    """Tests for KafkaException handling in _start_polling_async."""

    @pytest.mark.asyncio
    async def test_kafka_exception_during_poll_propagates(self, processor: MockAsyncProcessor) -> None:
        """KafkaException from poll() should propagate to trigger reconnection."""
        from cezzis_kafka.async_kafka_consumer import _start_polling_async

        mock_consumer = MagicMock(spec=Consumer)

        kafka_error = MagicMock()
        kafka_error.code.return_value = -1
        kafka_error.str.return_value = "Fatal error"

        with patch("cezzis_kafka.async_kafka_consumer._kafka_thread_pool"):
            # Make run_in_executor raise KafkaException
            async def raise_kafka_exception(*args, **kwargs):
                raise KafkaException(kafka_error)

            loop = asyncio.get_event_loop()
            with patch.object(loop, "run_in_executor", side_effect=raise_kafka_exception):
                with pytest.raises(KafkaException):
                    await _start_polling_async(mock_consumer, processor)


class TestConsumerStatsLogging:
    """Tests for stats callback broker connection logging."""

    @pytest.mark.asyncio
    async def test_logs_info_when_broker_state_transitions_to_up(self, processor: MockAsyncProcessor) -> None:
        """Stats callback should emit a connection-established info log for broker UP state."""
        mock_consumer = MagicMock(spec=Consumer)
        captured_config = {}

        def consumer_ctor(config):
            captured_config.update(config)
            return mock_consumer

        async def run_immediately(_executor, func, *args):
            return func(*args)

        loop = asyncio.get_event_loop()

        with (
            patch("cezzis_kafka.async_kafka_consumer.Consumer", side_effect=consumer_ctor),
            patch.object(loop, "run_in_executor", side_effect=run_immediately),
            patch("cezzis_kafka.async_kafka_consumer.logger.info") as mock_info,
        ):
            await _create_consumer_async(processor)

            stats_cb = captured_config["stats_cb"]
            stats_cb(
                json.dumps(
                    {
                        "brokers": {
                            "broker-1:19092/1": {
                                "state": "UP",
                                "nodeid": 1,
                            }
                        }
                    }
                )
            )

            assert any(call.args[0] == "Kafka broker connection established" for call in mock_info.call_args_list)
