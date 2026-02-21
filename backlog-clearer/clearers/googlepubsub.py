"""
High-throughput Google Pub/Sub backlog clearer.

Uses ``google-cloud-pubsub`` synchronous pull in multiple worker threads so
that several in-flight pull requests are outstanding at all times.  Each
worker issues a ``pull()`` request for up to *batch_size* messages and
immediately batch-acknowledges them.
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from .base import BaseClearer, ClearerStats

logger = logging.getLogger(__name__)

_EMPTY_POLL_LIMIT = 5


class GooglePubSubClearer(BaseClearer):
    """Drain a Google Cloud Pub/Sub subscription as fast as possible.

    Configuration keys (``config`` dict passed to ``__init__``):

    ===================== ===================================================
    Key                   Description
    ===================== ===================================================
    project_id            GCP project ID (required).
    subscription_id       Pub/Sub subscription name (required).
    credentials_file      Path to a service-account JSON key file.  If
                          omitted, Application Default Credentials are used.
    max_messages          Max messages per synchronous pull (default: 1 000).
    ===================== ===================================================
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._executor: Optional[ThreadPoolExecutor] = None
        self._subscription_path: Optional[str] = None

    # ------------------------------------------------------------------
    # BaseClearer interface
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        from google.cloud.pubsub_v1 import SubscriberClient  # type: ignore[import]
        from google.oauth2 import service_account  # type: ignore[import]

        project_id = self.config["project_id"]
        subscription_id = self.config["subscription_id"]

        if "credentials_file" in self.config:
            credentials = service_account.Credentials.from_service_account_file(
                self.config["credentials_file"],
                scopes=["https://www.googleapis.com/auth/pubsub"],
            )
            self._subscriber = SubscriberClient(credentials=credentials)
        else:
            self._subscriber = SubscriberClient()

        self._subscription_path = self._subscriber.subscription_path(
            project_id, subscription_id
        )

        max_workers = self.config.get("num_workers", 10) + 2
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="gpubsub-clearer",
        )
        self._running = True
        self.logger.info(
            "Google Pub/Sub clearer connected to %s.", self._subscription_path
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
        self.logger.info("Google Pub/Sub clear complete. %s", stats)
        return stats

    async def disconnect(self) -> None:
        self._running = False
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        if hasattr(self, "_subscriber"):
            self._subscriber.close()
        self.logger.info("Google Pub/Sub clearer disconnected.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _worker(self, batch_size: int, stop_when_empty: bool) -> None:
        """Thread-worker: pull → ack → repeat."""
        consecutive_empty = 0

        while self._running:
            try:
                response = self._subscriber.pull(
                    request={
                        "subscription": self._subscription_path,
                        "max_messages": batch_size,
                    },
                    timeout=10.0,
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Google Pub/Sub pull error: %s", exc)
                continue

            received = response.received_messages
            if not received:
                if stop_when_empty:
                    consecutive_empty += 1
                    if consecutive_empty >= _EMPTY_POLL_LIMIT:
                        break
                continue

            consecutive_empty = 0
            ack_ids = [m.ack_id for m in received]

            try:
                self._subscriber.acknowledge(
                    request={
                        "subscription": self._subscription_path,
                        "ack_ids": ack_ids,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Google Pub/Sub ack error: %s", exc)
                # Continue — messages will re-deliver after ack deadline.

            self._counter.add(len(ack_ids))
