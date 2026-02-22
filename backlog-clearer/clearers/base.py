"""
Base classes and shared utilities for pub-sub backlog clearers.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


class ThreadSafeCounter:
    """A thread-safe integer counter used for live throughput tracking."""

    __slots__ = ("_lock", "_value")

    def __init__(self, initial: int = 0) -> None:
        self._lock = threading.Lock()
        self._value = initial

    def add(self, n: int) -> None:
        with self._lock:
            self._value += n

    @property
    def value(self) -> int:
        with self._lock:
            return self._value


@dataclass
class ClearerStats:
    """Statistics produced by a backlog clearing run."""

    messages_consumed: int = 0
    #: Monotonic timestamp when clearing began.  Defaults to the moment this
    #: object is created; override when constructing from an earlier timestamp.
    start_time: float = field(default_factory=time.monotonic)
    end_time: Optional[float] = None

    @property
    def elapsed_seconds(self) -> float:
        end = self.end_time if self.end_time is not None else time.monotonic()
        return end - self.start_time

    @property
    def messages_per_second(self) -> float:
        elapsed = self.elapsed_seconds
        return self.messages_consumed / elapsed if elapsed > 0 else 0.0

    def __str__(self) -> str:
        return (
            f"Messages consumed: {self.messages_consumed:,} | "
            f"Elapsed: {self.elapsed_seconds:.1f}s | "
            f"Rate: {self.messages_per_second:,.0f} msg/s"
        )


class BaseClearer(ABC):
    """Abstract base class for all pub-sub backlog clearers.

    Concrete implementations must:
    - Connect to the pub-sub system in ``connect()``.
    - Drain the backlog in ``clear()``, updating ``_counter`` for every
      batch of messages that has been acknowledged/offset-committed.
    - Release resources in ``disconnect()``.
    """

    def __init__(self, config: dict) -> None:
        self.config = config
        self._running = False
        self._counter = ThreadSafeCounter()
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection(s) to the pub-sub system."""

    @abstractmethod
    async def clear(
        self,
        num_workers: int = 10,
        batch_size: int = 1000,
        stop_when_empty: bool = False,
    ) -> ClearerStats:
        """Consume and acknowledge messages as fast as possible.

        Args:
            num_workers: Number of concurrent consumer workers.
            batch_size: Number of messages to fetch per worker per iteration.
            stop_when_empty: Stop automatically when no more messages remain.

        Returns:
            :class:`ClearerStats` with final run statistics.
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """Close all connections and release resources."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """Signal the clearer to stop after the current batch."""
        self._running = False

    async def _log_progress(self, interval: float = 5.0) -> None:
        """Coroutine that logs throughput at regular intervals until stopped."""
        prev_count = 0
        prev_time = time.monotonic()
        while self._running:
            await asyncio.sleep(interval)
            now = time.monotonic()
            current = self._counter.value
            delta_msgs = current - prev_count
            delta_time = now - prev_time
            rate = delta_msgs / delta_time if delta_time > 0 else 0.0
            self.logger.info(
                "Progress — total: %s | window rate: %.0f msg/s",
                f"{current:,}",
                rate,
            )
            prev_count = current
            prev_time = now

    def _build_stats(self, start_time: float) -> ClearerStats:
        """Assemble a :class:`ClearerStats` object from internal counter."""
        return ClearerStats(
            messages_consumed=self._counter.value,
            start_time=start_time,
            end_time=time.monotonic(),
        )
