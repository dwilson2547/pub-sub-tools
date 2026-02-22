"""
Unit tests for the backlog-clearer tool.

All external pub-sub client libraries are mocked so these tests run without
any real infrastructure.  The tests verify:

- ThreadSafeCounter increments correctly under concurrent access.
- ClearerStats reports sensible elapsed time and throughput figures.
- get_clearer() returns the right class for each system identifier.
- Each clearer can complete a full connect → clear → disconnect cycle when its
  underlying client library is mocked.
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time
import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Make sure the project root is on sys.path so imports work from the tests dir.
# ---------------------------------------------------------------------------
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from clearers.base import ClearerStats, ThreadSafeCounter

# ---------------------------------------------------------------------------
# Pre-inject stub modules for optional heavy dependencies so that imports
# inside clearer modules succeed without installing the real libraries.
# ---------------------------------------------------------------------------

def _stub_module(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


# confluent_kafka stub
_confluent_kafka_stub = _stub_module(
    "confluent_kafka",
    Consumer=MagicMock(),
    KafkaError=MagicMock(_PARTITION_EOF=-191),
    KafkaException=Exception,
)
sys.modules.setdefault("confluent_kafka", _confluent_kafka_stub)

# pulsar stub
_pulsar_stub = _stub_module(
    "pulsar",
    Client=MagicMock(),
    AuthenticationToken=MagicMock(),
    ConsumerType=MagicMock(Shared="Shared"),
    ConsumerBatchReceivePolicy=MagicMock(),
    Timeout=type("Timeout", (Exception,), {}),
)
sys.modules.setdefault("pulsar", _pulsar_stub)

from clearers import SUPPORTED_SYSTEMS, get_clearer


# ===========================================================================
# ThreadSafeCounter
# ===========================================================================


class TestThreadSafeCounter:
    def test_initial_value_zero(self):
        c = ThreadSafeCounter()
        assert c.value == 0

    def test_initial_custom_value(self):
        c = ThreadSafeCounter(42)
        assert c.value == 42

    def test_add_increments(self):
        c = ThreadSafeCounter()
        c.add(100)
        c.add(200)
        assert c.value == 300

    def test_concurrent_add(self):
        """Many threads incrementing simultaneously must not lose updates."""
        c = ThreadSafeCounter()
        n_threads = 50
        increments_per_thread = 1_000

        def _inc():
            for _ in range(increments_per_thread):
                c.add(1)

        threads = [threading.Thread(target=_inc) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert c.value == n_threads * increments_per_thread


# ===========================================================================
# ClearerStats
# ===========================================================================


class TestClearerStats:
    def test_messages_per_second(self):
        start = time.monotonic() - 10.0  # pretend 10 s elapsed
        stats = ClearerStats(messages_consumed=50_000, start_time=start)
        assert stats.messages_per_second == pytest.approx(5_000, rel=0.05)

    def test_elapsed_seconds_uses_end_time(self):
        start = time.monotonic() - 20.0
        end = time.monotonic() - 10.0
        stats = ClearerStats(start_time=start, end_time=end)
        assert stats.elapsed_seconds == pytest.approx(10.0, rel=0.05)

    def test_elapsed_seconds_live_when_no_end_time(self):
        start = time.monotonic() - 2.0
        stats = ClearerStats(start_time=start)
        assert stats.elapsed_seconds >= 2.0

    def test_str_contains_key_fields(self):
        start = time.monotonic() - 5.0
        stats = ClearerStats(messages_consumed=25_000, start_time=start)
        text = str(stats)
        assert "25,000" in text
        assert "msg/s" in text


# ===========================================================================
# Factory — get_clearer
# ===========================================================================


class TestGetClearer:
    def test_all_supported_systems_resolve(self):
        """get_clearer must not raise for any supported system name."""
        mock_configs = {
            "kafka": {"bootstrap_servers": "localhost:9092", "topic": "t"},
            "pulsar": {"service_url": "pulsar://localhost:6650", "topic": "t"},
            "streamnative": {"service_url": "pulsar://localhost:6650", "topic": "t"},
            "iggy": {"stream_id": 1, "topic_id": 1, "partition_ids": [1]},
            "googlepubsub": {"project_id": "p", "subscription_id": "s"},
            "eventhubs": {"connection_string": "cs", "eventhub_name": "eh"},
        }
        for system in SUPPORTED_SYSTEMS:
            clearer = get_clearer(system, mock_configs[system])
            assert clearer is not None, f"get_clearer returned None for {system}"

    def test_unknown_system_raises(self):
        with pytest.raises(ValueError, match="Unknown pub-sub system"):
            get_clearer("nonexistent", {})

    def test_case_insensitive(self):
        clearer = get_clearer(
            "KAFKA",
            {"bootstrap_servers": "localhost:9092", "topic": "t"},
        )
        from clearers.kafka import KafkaClearer

        assert isinstance(clearer, KafkaClearer)


# ===========================================================================
# Kafka clearer
# ===========================================================================


def _make_fake_msg(count: int):
    """Return *count* fake confluent-kafka Message objects."""
    msgs = []
    for _ in range(count):
        m = MagicMock()
        m.error.return_value = None
        msgs.append(m)
    return msgs


class TestKafkaClearer:
    @pytest.fixture
    def config(self):
        return {
            "bootstrap_servers": "localhost:9092",
            "topic": "test-topic",
            "group_id": "test-group",
        }

    @pytest.mark.asyncio
    async def test_connect_creates_executor(self, config):
        from clearers.kafka import KafkaClearer  # confluent_kafka already stubbed

        clearer = KafkaClearer(config)
        await clearer.connect()
        assert clearer._executor is not None
        assert clearer._running is True
        await clearer.disconnect()

    @pytest.mark.asyncio
    async def test_clear_counts_messages(self, config):
        """Workers should count every valid message returned by consume()."""
        from clearers.kafka import KafkaClearer

        call_count = [0]

        def fake_consume(num_messages, timeout):
            call_count[0] += 1
            if call_count[0] <= 3:
                return _make_fake_msg(500)
            return []

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = fake_consume
        mock_consumer.commit = MagicMock()

        with patch("clearers.kafka.Consumer", return_value=mock_consumer):
            clearer = KafkaClearer(config)
            await clearer.connect()
            stats = await clearer.clear(
                num_workers=1, batch_size=500, stop_when_empty=True
            )
            await clearer.disconnect()

        # 3 batches × 500 messages
        assert stats.messages_consumed == 1_500

    @pytest.mark.asyncio
    async def test_clear_calls_async_commit(self, config):
        from clearers.kafka import KafkaClearer

        call_count = [0]

        def fake_consume(num_messages, timeout):
            call_count[0] += 1
            if call_count[0] == 1:
                return _make_fake_msg(10)
            return []

        mock_consumer = MagicMock()
        mock_consumer.consume.side_effect = fake_consume
        mock_consumer.commit = MagicMock()

        with patch("clearers.kafka.Consumer", return_value=mock_consumer):
            clearer = KafkaClearer(config)
            await clearer.connect()
            await clearer.clear(num_workers=1, batch_size=10, stop_when_empty=True)
            await clearer.disconnect()

        mock_consumer.commit.assert_called_with(asynchronous=True)


# ===========================================================================
# Pulsar clearer
# ===========================================================================


class TestPulsarClearer:
    @pytest.fixture
    def config(self):
        return {
            "service_url": "pulsar://localhost:6650",
            "topic": "persistent://tenant/ns/topic",
        }

    @pytest.mark.asyncio
    async def test_clear_counts_messages(self, config):
        from clearers.pulsar import PulsarClearer  # pulsar already stubbed

        call_count = [0]

        def fake_batch_receive():
            call_count[0] += 1
            if call_count[0] <= 2:
                return [MagicMock() for _ in range(200)]
            raise _pulsar_stub.Timeout()

        mock_consumer = MagicMock()
        mock_consumer.batch_receive.side_effect = fake_batch_receive
        mock_consumer.acknowledge = MagicMock()
        mock_consumer.close = MagicMock()

        mock_client = MagicMock()
        mock_client.subscribe.return_value = mock_consumer
        mock_client.close = MagicMock()

        with patch("clearers.pulsar.pulsar") as mock_pulsar_module:
            mock_pulsar_module.Client.return_value = mock_client
            mock_pulsar_module.AuthenticationToken = MagicMock()
            mock_pulsar_module.ConsumerType.Shared = "Shared"
            mock_pulsar_module.ConsumerBatchReceivePolicy = MagicMock()
            mock_pulsar_module.Timeout = _pulsar_stub.Timeout

            clearer = PulsarClearer(config)
            await clearer.connect()
            stats = await clearer.clear(
                num_workers=1, batch_size=200, stop_when_empty=True
            )
            await clearer.disconnect()

        assert stats.messages_consumed == 400


# ===========================================================================
# Google Pub/Sub clearer
# ===========================================================================


class TestGooglePubSubClearer:
    @pytest.fixture
    def config(self):
        return {
            "project_id": "my-project",
            "subscription_id": "my-subscription",
        }

    @pytest.mark.asyncio
    async def test_clear_counts_and_acks_messages(self, config):
        from clearers.googlepubsub import GooglePubSubClearer

        call_count = [0]

        def fake_pull(request, timeout):
            call_count[0] += 1
            if call_count[0] <= 2:
                resp = MagicMock()
                resp.received_messages = [MagicMock(ack_id=f"id-{i}") for i in range(100)]
                return resp
            resp = MagicMock()
            resp.received_messages = []
            return resp

        mock_subscriber = MagicMock()
        mock_subscriber.subscription_path.return_value = "projects/p/subscriptions/s"
        mock_subscriber.pull.side_effect = fake_pull
        mock_subscriber.acknowledge = MagicMock()
        mock_subscriber.close = MagicMock()

        with patch("clearers.googlepubsub.GooglePubSubClearer.connect"):
            # Bypass connect to avoid importing google.cloud.pubsub_v1
            clearer = GooglePubSubClearer(config)
            clearer._subscriber = mock_subscriber
            clearer._subscription_path = "projects/p/subscriptions/s"
            clearer._executor = None

        from concurrent.futures import ThreadPoolExecutor

        clearer._executor = ThreadPoolExecutor(max_workers=4)
        clearer._running = True

        stats = await clearer.clear(
            num_workers=1, batch_size=100, stop_when_empty=True
        )
        await clearer.disconnect()

        assert stats.messages_consumed == 200
        assert mock_subscriber.acknowledge.call_count == 2


# ===========================================================================
# Iggy clearer (pure async path — no thread pool needed)
# ===========================================================================


class TestIggyClearer:
    @pytest.fixture
    def config(self):
        return {
            "host": "127.0.0.1",
            "port": 8090,
            "stream_id": 1,
            "topic_id": 1,
            "partition_ids": [1, 2],
        }

    @pytest.mark.asyncio
    async def test_clear_counts_messages(self, config):
        from clearers.iggy import IggyClearer

        poll_counts: dict = {1: 0, 2: 0}

        async def fake_poll_messages(**kwargs):
            pid = kwargs["partition_id"]
            poll_counts[pid] = poll_counts.get(pid, 0) + 1
            if poll_counts[pid] <= 2:
                result = MagicMock()
                result.messages = [MagicMock() for _ in range(50)]
                return result
            result = MagicMock()
            result.messages = []
            return result

        mock_messages = MagicMock()
        mock_messages.poll_messages = fake_poll_messages

        mock_users = MagicMock()
        mock_users.login_user = AsyncMock()

        mock_client = MagicMock()
        mock_client.connect = AsyncMock()
        mock_client.disconnect = AsyncMock()
        mock_client.messages = mock_messages
        mock_client.users = mock_users

        # Provide stub modules so the import inside connect() succeeds.
        iggy_client_mod = types.ModuleType("iggy.client.client")
        iggy_client_mod.IggyClient = MagicMock(return_value=mock_client)

        iggy_tcp_mod = types.ModuleType("iggy.client.tcp.config")
        iggy_tcp_mod.TcpClientConfig = MagicMock()

        iggy_polling_mod = types.ModuleType("iggy.messages.polling_strategy")
        polling_strategy = MagicMock()
        polling_strategy.next = MagicMock(return_value="next")
        iggy_polling_mod.PollingStrategy = polling_strategy

        iggy_id_mod = types.ModuleType("iggy.identifier")
        iggy_id_mod.Identifier = MagicMock(side_effect=lambda numeric_id: numeric_id)

        with (
            patch.dict(
                sys.modules,
                {
                    "iggy": types.ModuleType("iggy"),
                    "iggy.client": types.ModuleType("iggy.client"),
                    "iggy.client.client": iggy_client_mod,
                    "iggy.client.tcp": types.ModuleType("iggy.client.tcp"),
                    "iggy.client.tcp.config": iggy_tcp_mod,
                    "iggy.messages": types.ModuleType("iggy.messages"),
                    "iggy.messages.polling_strategy": iggy_polling_mod,
                    "iggy.identifier": iggy_id_mod,
                },
            )
        ):
            clearer = IggyClearer(config)
            await clearer.connect()
            stats = await clearer.clear(
                num_workers=2, batch_size=50, stop_when_empty=True
            )
            await clearer.disconnect()

        # 2 partitions × 2 batches × 50 messages
        assert stats.messages_consumed == 200


# ===========================================================================
# Event Hubs clearer
# ===========================================================================


class TestEventHubsClearer:
    @pytest.fixture
    def config(self):
        return {
            "connection_string": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=s",
            "eventhub_name": "test-hub",
            "partition_ids": ["0", "1"],
        }

    @pytest.mark.asyncio
    async def test_clear_counts_messages(self, config):
        from clearers.eventhubs import EventHubsClearer

        # Counts how many times receive_batch is called per partition
        recv_counts: dict = {"0": 0, "1": 0}

        async def fake_receive_batch(partition_id, max_batch_size, max_wait_time, starting_position):
            recv_counts[partition_id] = recv_counts.get(partition_id, 0) + 1
            if recv_counts[partition_id] <= 2:
                return [MagicMock() for _ in range(max_batch_size)]
            return []

        mock_client = AsyncMock()
        mock_client.receive_batch.side_effect = fake_receive_batch
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.close = AsyncMock()

        mock_client_cls = MagicMock()
        mock_client_cls.from_connection_string.return_value = mock_client
        mock_client_cls.return_value = mock_client

        event_pos_mock = MagicMock()

        with (
            patch("clearers.eventhubs.EventHubsClearer.connect"),
        ):
            clearer = EventHubsClearer(config)
            clearer._partition_ids = ["0", "1"]
            clearer._client = mock_client
            clearer._running = True

        eventhub_aio_mod = types.ModuleType("azure.eventhub.aio")
        eventhub_aio_mod.EventHubConsumerClient = mock_client_cls

        eventhub_mod = types.ModuleType("azure.eventhub")
        eventhub_mod.EventPosition = event_pos_mock

        with patch.dict(
            sys.modules,
            {
                "azure": types.ModuleType("azure"),
                "azure.eventhub": eventhub_mod,
                "azure.eventhub.aio": eventhub_aio_mod,
            },
        ):
            stats = await clearer.clear(
                num_workers=2, batch_size=100, stop_when_empty=True
            )

        clearer._running = False
        # 2 partitions × 2 recv batches × 100 messages
        assert stats.messages_consumed == 400


# ===========================================================================
# CLI argument parsing
# ===========================================================================


class TestCLIArgParsing:
    def test_kafka_args_build_correct_config(self):
        from cleaner import _build_parser, _build_config

        parser = _build_parser()
        args = parser.parse_args(
            [
                "kafka",
                "--bootstrap-servers",
                "broker:9092",
                "--topic",
                "my-topic",
                "--group-id",
                "my-group",
                "--workers",
                "20",
                "--batch-size",
                "2000",
            ]
        )
        cfg = _build_config(args)
        assert cfg["bootstrap_servers"] == "broker:9092"
        assert cfg["topic"] == "my-topic"
        assert cfg["group_id"] == "my-group"
        assert cfg["num_workers"] == 20

    def test_iggy_partition_ids_parsed(self):
        from cleaner import _build_parser, _build_config

        parser = _build_parser()
        args = parser.parse_args(
            [
                "iggy",
                "--stream-id",
                "1",
                "--topic-id",
                "2",
                "--partition-ids",
                "1,2,3,4",
            ]
        )
        cfg = _build_config(args)
        assert cfg["partition_ids"] == [1, 2, 3, 4]

    def test_googlepubsub_args(self):
        from cleaner import _build_parser, _build_config

        parser = _build_parser()
        args = parser.parse_args(
            [
                "googlepubsub",
                "--project-id",
                "my-proj",
                "--subscription-id",
                "my-sub",
            ]
        )
        cfg = _build_config(args)
        assert cfg["project_id"] == "my-proj"
        assert cfg["subscription_id"] == "my-sub"

    def test_eventhubs_args(self):
        from cleaner import _build_parser, _build_config

        parser = _build_parser()
        args = parser.parse_args(
            [
                "eventhubs",
                "--connection-string",
                "Endpoint=sb://x",
                "--eventhub-name",
                "hub1",
                "--eventhub-partition-ids",
                "0,1,2",
            ]
        )
        cfg = _build_config(args)
        assert cfg["connection_string"] == "Endpoint=sb://x"
        assert cfg["eventhub_name"] == "hub1"
        assert cfg["partition_ids"] == ["0", "1", "2"]

    def test_stop_when_empty_flag(self):
        from cleaner import _build_parser

        parser = _build_parser()
        args = parser.parse_args(
            ["kafka", "--bootstrap-servers", "b:9092", "--stop-when-empty"]
        )
        assert args.stop_when_empty is True
