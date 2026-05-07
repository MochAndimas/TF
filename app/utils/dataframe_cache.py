"""Small in-process cache for expensive pandas DataFrame reads."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Hashable
from dataclasses import dataclass
from threading import RLock
from time import monotonic

import pandas as pd


CacheKey = tuple[Hashable, ...]


@dataclass(frozen=True)
class DataFrameCacheStats:
    """Snapshot of DataFrame cache usage."""

    entries: int
    max_entries: int
    ttl_seconds: int


@dataclass
class _CacheEntry:
    value: pd.DataFrame
    expires_at: float


class DataFrameCache:
    """Bounded TTL cache that stores defensive copies of DataFrames."""

    def __init__(self, *, max_entries: int, ttl_seconds: int) -> None:
        self.max_entries = max(1, int(max_entries))
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._entries: OrderedDict[CacheKey, _CacheEntry] = OrderedDict()
        self._lock = RLock()

    def get(self, key: CacheKey) -> pd.DataFrame | None:
        """Return a copy of the cached DataFrame when the entry is still fresh."""
        now = monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return entry.value.copy(deep=True)

    def set(self, key: CacheKey, value: pd.DataFrame) -> pd.DataFrame:
        """Store a defensive copy and return a copy for immediate caller use."""
        cached_value = value.copy(deep=True)
        with self._lock:
            self._entries[key] = _CacheEntry(
                value=cached_value,
                expires_at=monotonic() + self.ttl_seconds,
            )
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
        return cached_value.copy(deep=True)

    def clear(self, prefix: CacheKey | None = None) -> int:
        """Clear all entries or entries whose key starts with ``prefix``."""
        with self._lock:
            if prefix is None:
                removed = len(self._entries)
                self._entries.clear()
                return removed

            keys = [key for key in self._entries if key[: len(prefix)] == prefix]
            for key in keys:
                self._entries.pop(key, None)
            return len(keys)

    def stats(self) -> DataFrameCacheStats:
        """Return cache size/configuration without exposing cached values."""
        with self._lock:
            return DataFrameCacheStats(
                entries=len(self._entries),
                max_entries=self.max_entries,
                ttl_seconds=self.ttl_seconds,
            )
