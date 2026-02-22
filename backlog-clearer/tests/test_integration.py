"""
Integration tests for the backlog-clearer tool using testcontainers.

These tests spin up real Kafka and Pulsar containers, publish a small number of
messages, and then verify that the corresponding clearer drains every published
message.  They are marked with ``pytest.mark.integration`` and require a
working Docker daemon.

Run only these tests with::

    pytest -m integration tests/test_integration.py

Skip them (for example in lightweight CI without Docker) with::

    pytest -m "not integration"
"""

from __future__ import annotations

import os
import sys
import time
from typing import Generator

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Remove any stubs injected at module-level by test_clearers.py so that the
# real client libraries (confluent-kafka, pulsar-client) and their dependent
# clearer modules are re-imported fresh with real implementations.
# ---------------------------------------------------------------------------
for _mod in ("confluent_kafka", "pulsar", "clearers.kafka", "clearers.pulsar"):
    sys.modules.pop(_mod, None)

# ---------------------------------------------------------------------------
# pytest markers
# ---------------------------------------------------------------------------
pytestmark = pytest.mark.integration


# ===========================================================================
# Kafka integration test
# ===========================================================================


@pytest.fixture(scope="module")
def kafka_container():
    """Start a single-broker Kafka container for the whole module."""
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer().with_kraft() as container:
        yield container


@pytest.mark.asyncio
async def test_kafka_clearer_drains_topic(kafka_container):
    """Publish N messages to a Kafka topic then clear the backlog and assert
    that all messages were consumed."""
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    from clearers.kafka import KafkaClearer

    bootstrap_servers = kafka_container.get_bootstrap_server()
    topic = "test-backlog-clear"
    num_messages = 50
    group_id = "backlog-clearer-test"

    # Create the topic explicitly and wait until it is fully available.
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])

    # Poll metadata until the topic appears (avoids a fixed sleep).
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        metadata = admin.list_topics(timeout=2)
        if topic in metadata.topics:
            break
        time.sleep(0.2)

    # Produce messages.
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for i in range(num_messages):
        producer.produce(topic, value=f"msg-{i}".encode())
    producer.flush()

    # Run the clearer.
    config = {
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "group_id": group_id,
        "auto_offset_reset": "earliest",
    }
    clearer = KafkaClearer(config)
    await clearer.connect()
    stats = await clearer.clear(num_workers=1, batch_size=100, stop_when_empty=True)
    await clearer.disconnect()

    assert stats.messages_consumed == num_messages, (
        f"Expected {num_messages} messages consumed, got {stats.messages_consumed}"
    )


# ===========================================================================
# Pulsar integration test
# ===========================================================================


@pytest.fixture(scope="module")
def pulsar_container():
    """Start an Apache Pulsar standalone container for the whole module."""
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    with (
        DockerContainer("apachepulsar/pulsar:3.2.0")
        .with_command("bin/pulsar standalone")
        .with_exposed_ports(6650, 8080)
        .waiting_for(LogMessageWaitStrategy("messaging service is ready"))
    ) as container:
        yield container


@pytest.mark.asyncio
async def test_pulsar_clearer_drains_topic(pulsar_container):
    """Publish N messages to a Pulsar topic then clear the backlog and assert
    that all messages were consumed."""
    import pulsar as pulsar_client

    from clearers.pulsar import PulsarClearer

    host = pulsar_container.get_container_host_ip()
    port = pulsar_container.get_exposed_port(6650)
    service_url = f"pulsar://{host}:{port}"
    topic = "persistent://public/default/test-backlog-clear"
    subscription = "backlog-clearer-test"
    num_messages = 50

    # Pre-create the subscription at the *earliest* position so messages
    # published afterwards are captured in the backlog.
    client = pulsar_client.Client(service_url)
    consumer = client.subscribe(
        topic,
        subscription,
        consumer_type=pulsar_client.ConsumerType.Shared,
        initial_position=pulsar_client.InitialPosition.Earliest,
    )

    # Publish messages.
    producer = client.create_producer(topic)
    for i in range(num_messages):
        producer.send(f"msg-{i}".encode())
    producer.close()

    # Close the consumer without acknowledging so the backlog stays intact.
    consumer.close()
    client.close()

    # Run the clearer.
    config = {
        "service_url": service_url,
        "topic": topic,
        "subscription": subscription,
    }
    clearer = PulsarClearer(config)
    await clearer.connect()
    stats = await clearer.clear(num_workers=1, batch_size=100, stop_when_empty=True)
    await clearer.disconnect()

    assert stats.messages_consumed == num_messages, (
        f"Expected {num_messages} messages consumed, got {stats.messages_consumed}"
    )
