"""
High-throughput Kafka backlog clearer.

Uses ``confluent-kafka`` (librdkafka Python bindings) for maximum throughput.
Each worker runs its own :class:`confluent_kafka.Consumer` in a thread-pool
executor so the GIL never becomes a bottleneck.  Workers share the same
consumer *group*, letting Kafka distribute partitions automatically.
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Union

from confluent_kafka import Consumer, KafkaError, KafkaException

from .base import BaseClearer, ClearerStats

logger = logging.getLogger(__name__)

# How many consecutive empty polls before a worker decides the topic is drained.
_EMPTY_POLL_LIMIT = 5


class KafkaClearer(BaseClearer):
    """Drain Kafka topics as fast as possible.

    Configuration keys (``config`` dict passed to ``__init__``):

    ==================== ===================================================
    Key                  Description
    ==================== ===================================================
    bootstrap_servers    Comma-separated broker list (required).
    topic                Topic name or list of topic names (required).
    group_id             Consumer group ID (default: ``backlog-clearer``).
    auto_offset_reset    ``earliest`` (default) or ``latest``.
    security_protocol    ``PLAINTEXT``, ``SSL``, ``SASL_PLAINTEXT``,
                         ``SASL_SSL`` (optional).
    sasl_mechanism       ``PLAIN``, ``SCRAM-SHA-256``, ``GSSAPI``, etc.
    sasl_username        SASL username (optional).
    sasl_password        SASL password (optional).
    ssl_ca_location      Path to CA certificate file (optional).
    Any key containing a ``"."`` is forwarded directly to librdkafka.
    ==================== ===================================================
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._executor: Optional[ThreadPoolExecutor] = None

    # ------------------------------------------------------------------
    # BaseClearer interface
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        max_workers = self.config.get("num_workers", 10) + 2
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="kafka-clearer",
        )
        self._running = True
        self.logger.info("Kafka clearer ready (thread pool: %d).", max_workers)

    async def clear(
        self,
        num_workers: int = 10,
        batch_size: int = 1000,
        stop_when_empty: bool = False,
    ) -> ClearerStats:
        self._counter = self._counter.__class__()  # reset
        self._running = True
        start_time = time.monotonic()

        topics: List[str] = self.config["topic"]
        if isinstance(topics, str):
            topics = [topics]

        loop = asyncio.get_event_loop()
        progress_task = asyncio.create_task(self._log_progress())

        worker_futures = [
            loop.run_in_executor(
                self._executor,
                self._worker,
                topics,
                batch_size,
                stop_when_empty,
            )
            for _ in range(num_workers)
        ]

        await asyncio.gather(*worker_futures)
        self._running = False
        progress_task.cancel()

        stats = self._build_stats(start_time)
        self.logger.info("Kafka clear complete. %s", stats)
        return stats

    async def disconnect(self) -> None:
        self._running = False
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        self.logger.info("Kafka clearer disconnected.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_consumer_config(self) -> dict:
        cfg: dict = {
            "bootstrap.servers": self.config["bootstrap_servers"],
            "group.id": self.config.get("group_id", "backlog-clearer"),
            "auto.offset.reset": self.config.get("auto_offset_reset", "earliest"),
            "enable.auto.commit": False,
            # Fetch tuning for high throughput
            "fetch.min.bytes": 1,
            "fetch.max.bytes": 52_428_800,       # 50 MiB
            "max.partition.fetch.bytes": 10_485_760,  # 10 MiB per partition
            "queued.max.messages.kbytes": 131_072,    # 128 MiB in-memory queue
            "receive.message.max.bytes": 67_108_864,  # 64 MiB max message
            "session.timeout.ms": 30_000,
            "heartbeat.interval.ms": 5_000,
        }

        # Optional security settings
        _security_keys = {
            "security_protocol": "security.protocol",
            "sasl_mechanism": "sasl.mechanism",
            "sasl_username": "sasl.username",
            "sasl_password": "sasl.password",
            "ssl_ca_location": "ssl.ca.location",
        }
        for py_key, rdkafka_key in _security_keys.items():
            if py_key in self.config:
                cfg[rdkafka_key] = self.config[py_key]

        # Forward any raw librdkafka keys (keys that already contain ".")
        for key, value in self.config.items():
            if "." in key:
                cfg[key] = value

        return cfg

    def _worker(
        self,
        topics: List[str],
        batch_size: int,
        stop_when_empty: bool,
    ) -> None:
        """Thread-worker: poll, ack, repeat."""
        consumer = Consumer(self._build_consumer_config())
        consumer.subscribe(topics)
        consecutive_empty = 0

        try:
            while self._running:
                messages = consumer.consume(num_messages=batch_size, timeout=1.0)

                if not messages:
                    if stop_when_empty:
                        consecutive_empty += 1
                        if consecutive_empty >= _EMPTY_POLL_LIMIT:
                            break
                    continue

                consecutive_empty = 0

                valid: List = []
                for msg in messages:
                    err = msg.error()
                    if err is None:
                        valid.append(msg)
                    elif err.code() != KafkaError._PARTITION_EOF:
                        self.logger.warning("Kafka consumer error: %s", err)

                if valid:
                    consumer.commit(asynchronous=True)
                    self._counter.add(len(valid))

        except KafkaException as exc:
            self.logger.error("Kafka worker fatal error: %s", exc)
        finally:
            consumer.close()
