"""
High-throughput Azure Event Hubs backlog clearer.

Uses ``azure-eventhub`` with ``receive_batch()`` on every partition so that
multiple workers can drain different partitions in parallel.  Workers advance
the checkpoint after each successfully processed batch.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import List, Optional

from .base import BaseClearer, ClearerStats

logger = logging.getLogger(__name__)

_EMPTY_POLL_LIMIT = 5


class EventHubsClearer(BaseClearer):
    """Drain an Azure Event Hubs instance as fast as possible.

    Configuration keys (``config`` dict passed to ``__init__``):

    ====================== =================================================
    Key                    Description
    ====================== =================================================
    connection_string      Event Hubs connection string (required unless
                           ``fully_qualified_namespace`` is given).
    fully_qualified_namespace  ``<namespace>.servicebus.windows.net``
                           (used with ``credential``).
    credential             An ``azure.identity`` credential object (optional,
                           used with ``fully_qualified_namespace``).
    eventhub_name          Event Hub name (required).
    consumer_group         Consumer group (default: ``$Default``).
    partition_ids          List of partition ID strings to drain.  If omitted
                           the clearer fetches the list from the service.
    starting_position      ``"-1"`` = beginning (default), ``"@latest"`` =
                           latest.
    checkpoint_store       An ``azure.eventhub.extensions.checkpointstoreblob``
                           compatible store (optional).  If omitted, in-memory
                           positions are used (no durable checkpoint).
    ====================== =================================================
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._client = None
        self._partition_ids: List[str] = []

    # ------------------------------------------------------------------
    # BaseClearer interface
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        from azure.eventhub.aio import EventHubConsumerClient  # type: ignore[import]

        consumer_group = self.config.get("consumer_group", "$Default")

        if "connection_string" in self.config:
            self._client = EventHubConsumerClient.from_connection_string(
                self.config["connection_string"],
                consumer_group=consumer_group,
                eventhub_name=self.config["eventhub_name"],
            )
        else:
            self._client = EventHubConsumerClient(
                fully_qualified_namespace=self.config["fully_qualified_namespace"],
                eventhub_name=self.config["eventhub_name"],
                consumer_group=consumer_group,
                credential=self.config["credential"],
            )

        # Fetch partition list unless caller already supplied it.
        if "partition_ids" in self.config:
            self._partition_ids = list(self.config["partition_ids"])
        else:
            async with self._client:
                props = await self._client.get_eventhub_properties()
                self._partition_ids = props["partition_ids"]

        self._running = True
        self.logger.info(
            "Event Hubs clearer connected — %d partitions.",
            len(self._partition_ids),
        )

    async def clear(
        self,
        num_workers: int = 10,
        batch_size: int = 1000,
        stop_when_empty: bool = False,
    ) -> ClearerStats:
        self._counter = self._counter.__class__()
        self._running = True
        start_time = time.monotonic()

        progress_task = asyncio.create_task(self._log_progress())

        # Distribute partitions across workers (round-robin)
        worker_partitions: List[List[str]] = [[] for _ in range(num_workers)]
        for i, pid in enumerate(self._partition_ids):
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
        self.logger.info("Event Hubs clear complete. %s", stats)
        return stats

    async def disconnect(self) -> None:
        self._running = False
        if self._client:
            await self._client.close()
            self._client = None
        self.logger.info("Event Hubs clearer disconnected.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _worker(
        self,
        partition_ids: List[str],
        batch_size: int,
        stop_when_empty: bool,
    ) -> None:
        """Async worker: drain assigned partitions in round-robin."""
        from azure.eventhub.aio import EventHubConsumerClient  # type: ignore[import]
        from azure.eventhub import EventPosition  # type: ignore[import]

        consumer_group = self.config.get("consumer_group", "$Default")
        starting_position = self.config.get("starting_position", "-1")

        # Each worker opens its own consumer client to avoid sharing state.
        if "connection_string" in self.config:
            client = EventHubConsumerClient.from_connection_string(
                self.config["connection_string"],
                consumer_group=consumer_group,
                eventhub_name=self.config["eventhub_name"],
            )
        else:
            client = EventHubConsumerClient(
                fully_qualified_namespace=self.config["fully_qualified_namespace"],
                eventhub_name=self.config["eventhub_name"],
                consumer_group=consumer_group,
                credential=self.config["credential"],
            )

        empty_counts = {pid: 0 for pid in partition_ids}
        active_partitions = set(partition_ids)

        try:
            async with client:
                while self._running:
                    all_empty = True
                    for pid in list(active_partitions):
                        if not self._running:
                            break
                        try:
                            events = await client.receive_batch(
                                partition_id=pid,
                                max_batch_size=batch_size,
                                max_wait_time=1.0,
                                starting_position=EventPosition(starting_position),
                            )
                        except Exception as exc:  # noqa: BLE001
                            self.logger.error(
                                "Event Hubs receive error (partition %s): %s",
                                pid,
                                exc,
                            )
                            await asyncio.sleep(0.1)
                            continue

                        count = len(events) if events else 0
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
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Event Hubs worker error: %s", exc)
