"""
High-throughput Apache Pulsar / StreamNative backlog clearer.

Uses ``pulsar-client`` with a *Shared* subscription so that multiple consumer
instances can drain the same subscription in parallel.  Each worker runs in a
thread-pool executor and calls ``batch_receive()`` in a tight loop, then
issues a cumulative acknowledge for each batch.

StreamNative is a managed Apache Pulsar service; pass JWT credentials via
``token`` in the config dict and the same clearer handles it transparently.
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pulsar

from .base import BaseClearer, ClearerStats

logger = logging.getLogger(__name__)

_EMPTY_POLL_LIMIT = 5


class PulsarClearer(BaseClearer):
    """Drain an Apache Pulsar / StreamNative topic as fast as possible.

    Configuration keys (``config`` dict passed to ``__init__``):

    ==================== ===================================================
    Key                  Description
    ==================== ===================================================
    service_url          Pulsar broker URL, e.g. ``pulsar://localhost:6650``
                         or ``pulsar+ssl://…`` (required).
    topic                Fully-qualified topic name (required).
    subscription         Subscription name (default: ``backlog-clearer``).
    token                JWT authentication token (StreamNative / TLS auth).
    tls_trust_certs_file Path to CA certificate for TLS brokers (optional).
    batch_receive_size   Max messages per ``batch_receive()`` call
                         (default: 1 000).
    receiver_queue_size  Per-consumer prefetch queue size (default: 10 000).
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
            thread_name_prefix="pulsar-clearer",
        )
        self._running = True
        self.logger.info("Pulsar clearer ready (thread pool: %d).", max_workers)

    async def clear(
        self,
        num_workers: int = 10,
        batch_size: int = 1000,
        stop_when_empty: bool = False,
    ) -> ClearerStats:
        self._counter = self._counter.__class__()
        self._running = True
        start_time = time.monotonic()

        loop = asyncio.get_event_loop()
        progress_task = asyncio.create_task(self._log_progress())

        worker_futures = [
            loop.run_in_executor(
                self._executor,
                self._worker,
                batch_size,
                stop_when_empty,
            )
            for _ in range(num_workers)
        ]

        await asyncio.gather(*worker_futures)
        self._running = False
        progress_task.cancel()

        stats = self._build_stats(start_time)
        self.logger.info("Pulsar clear complete. %s", stats)
        return stats

    async def disconnect(self) -> None:
        self._running = False
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        self.logger.info("Pulsar clearer disconnected.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_client(self) -> pulsar.Client:
        kwargs: dict = {}
        if "token" in self.config:
            kwargs["authentication"] = pulsar.AuthenticationToken(
                self.config["token"]
            )
        if "tls_trust_certs_file" in self.config:
            kwargs["tls_trust_certs_file_path"] = self.config[
                "tls_trust_certs_file"
            ]
            kwargs["tls_validate_hostname"] = self.config.get(
                "tls_validate_hostname", True
            )
        return pulsar.Client(self.config["service_url"], **kwargs)

    def _worker(self, batch_size: int, stop_when_empty: bool) -> None:
        """Thread-worker: batch receive → cumulative ack → repeat."""
        client = self._build_client()
        receiver_queue = self.config.get("receiver_queue_size", 10_000)
        subscription = self.config.get("subscription", "backlog-clearer")

        policy = pulsar.ConsumerType.Shared
        batch_policy = pulsar.BatchReceivePolicy(
            max_num_messages=batch_size,
            timeout_ms=1_000,
        )

        consumer = client.subscribe(
            self.config["topic"],
            subscription,
            consumer_type=policy,
            receiver_queue_size=receiver_queue,
            batch_receive_policy=batch_policy,
        )

        consecutive_empty = 0
        try:
            while self._running:
                try:
                    msgs = consumer.batch_receive()
                except pulsar.Timeout:
                    if stop_when_empty:
                        consecutive_empty += 1
                        if consecutive_empty >= _EMPTY_POLL_LIMIT:
                            break
                    continue

                if not msgs:
                    if stop_when_empty:
                        consecutive_empty += 1
                        if consecutive_empty >= _EMPTY_POLL_LIMIT:
                            break
                    continue

                consecutive_empty = 0
                count = len(msgs)

                # Cumulative ack on the last message of the batch —
                # cheaper than individual acks.
                consumer.acknowledge_cumulative(msgs[-1])
                self._counter.add(count)

        except Exception as exc:  # noqa: BLE001
            self.logger.error("Pulsar worker error: %s", exc)
        finally:
            try:
                consumer.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass
