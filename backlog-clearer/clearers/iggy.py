"""
High-throughput Iggy backlog clearer.

Uses the ``iggy-py`` async client to poll messages from a stream/topic in
batches and commit offsets after each batch.  Multiple asyncio tasks drain
different partitions concurrently.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Optional

from .base import BaseClearer, ClearerStats

logger = logging.getLogger(__name__)

_EMPTY_POLL_LIMIT = 5


class IggyClearer(BaseClearer):
    """Drain an Iggy stream/topic as fast as possible.

    Configuration keys (``config`` dict passed to ``__init__``):

    ==================== ===================================================
    Key                  Description
    ==================== ===================================================
    host                 Iggy server host (default: ``127.0.0.1``).
    port                 Iggy TCP port (default: ``8090``).
    username             Username for authentication (default: ``iggy``).
    password             Password for authentication (default: ``iggy``).
    stream_id            Numeric stream ID (required).
    topic_id             Numeric topic ID (required).
    partition_ids        List of partition IDs to drain (required).
    consumer_name        Consumer name used for offset tracking
                         (default: ``backlog-clearer``).
    ==================== ===================================================
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._client = None

    # ------------------------------------------------------------------
    # BaseClearer interface
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        # Import here so the library is only required when actually used.
        from iggy.client.client import IggyClient  # type: ignore[import]
        from iggy.client.tcp.config import TcpClientConfig  # type: ignore[import]

        host = self.config.get("host", "127.0.0.1")
        port = self.config.get("port", 8090)

        cfg = TcpClientConfig(host=host, port=port)
        self._client = IggyClient(transport_config=cfg)
        await self._client.connect()

        username = self.config.get("username", "iggy")
        password = self.config.get("password", "iggy")
        await self._client.users.login_user(username=username, password=password)

        self._running = True
        self.logger.info("Iggy clearer connected to %s:%d.", host, port)

    async def clear(
        self,
        num_workers: int = 10,
        batch_size: int = 1000,
        stop_when_empty: bool = False,
    ) -> ClearerStats:
        self._counter = self._counter.__class__()
        self._running = True
        start_time = time.monotonic()

        partition_ids: List[int] = self.config["partition_ids"]
        progress_task = asyncio.create_task(self._log_progress())

        # Distribute partitions across workers (round-robin)
        worker_partitions: List[List[int]] = [[] for _ in range(num_workers)]
        for i, pid in enumerate(partition_ids):
            worker_partitions[i % num_workers].append(pid)

        tasks = [
            asyncio.create_task(
                self._worker(partitions, batch_size, stop_when_empty)
            )
            for partitions in worker_partitions
            if partitions
        ]

        await asyncio.gather(*tasks)
        self._running = False
        progress_task.cancel()

        stats = self._build_stats(start_time)
        self.logger.info("Iggy clear complete. %s", stats)
        return stats

    async def disconnect(self) -> None:
        self._running = False
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:  # noqa: BLE001
                pass
            self._client = None
        self.logger.info("Iggy clearer disconnected.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _worker(
        self,
        partition_ids: List[int],
        batch_size: int,
        stop_when_empty: bool,
    ) -> None:
        """Async worker: poll each partition in round-robin."""
        from iggy.messages.polling_strategy import PollingStrategy  # type: ignore[import]
        from iggy.identifier import Identifier  # type: ignore[import]

        stream_id = Identifier(numeric_id=self.config["stream_id"])
        topic_id = Identifier(numeric_id=self.config["topic_id"])
        consumer_name = self.config.get("consumer_name", "backlog-clearer")

        empty_counts = {pid: 0 for pid in partition_ids}
        active_partitions = set(partition_ids)

        while self._running:
            all_empty = True
            for pid in list(active_partitions):
                if not self._running:
                    break

                try:
                    result = await self._client.messages.poll_messages(
                        stream_id=stream_id,
                        topic_id=topic_id,
                        partition_id=pid,
                        consumer=consumer_name,
                        polling_strategy=PollingStrategy.next(),
                        count=batch_size,
                        auto_commit=True,
                    )
                    count = len(result.messages) if result and result.messages else 0
                except Exception as exc:  # noqa: BLE001
                    self.logger.error(
                        "Iggy poll error (partition %d): %s", pid, exc
                    )
                    await asyncio.sleep(0.1)
                    continue

                if count:
                    all_empty = False
                    empty_counts[pid] = 0
                    self._counter.add(count)
                else:
                    empty_counts[pid] += 1
                    if (
                        stop_when_empty
                        and empty_counts[pid] >= _EMPTY_POLL_LIMIT
                    ):
                        active_partitions.discard(pid)
                        if not active_partitions:
                            return

            if stop_when_empty and all_empty:
                await asyncio.sleep(0.05)
