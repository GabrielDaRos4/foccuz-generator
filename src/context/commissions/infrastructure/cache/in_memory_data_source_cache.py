import logging
from dataclasses import dataclass, field

import pandas as pd

from src.context.commissions.domain.value_objects import CacheKey

from .cache_stats import CacheStats

logger = logging.getLogger(__name__)


@dataclass
class InMemoryDataSourceCache:
    _cache: dict[str, pd.DataFrame] = field(default_factory=dict)
    _hits: int = 0
    _misses: int = 0

    def get(self, key: CacheKey) -> pd.DataFrame | None:
        key_str = str(key)
        if key_str in self._cache:
            self._hits += 1
            df = self._cache[key_str]
            logger.info(f"Cache HIT for {key_str}: {len(df)} rows")
            return df.copy()

        self._misses += 1
        logger.debug(f"Cache MISS for {key_str}")
        return None

    def set(self, key: CacheKey, data: pd.DataFrame) -> None:
        key_str = str(key)
        self._cache[key_str] = data.copy()
        logger.info(f"Cached {key_str}: {len(data)} rows")

    def contains(self, key: CacheKey) -> bool:
        return str(key) in self._cache

    def clear(self) -> None:
        entries = len(self._cache)
        self._cache.clear()
        if entries > 0:
            logger.info(f"Cache cleared: removed {entries} entries")
        self._hits = 0
        self._misses = 0

    def get_stats(self) -> CacheStats:
        total_rows = sum(len(df) for df in self._cache.values())
        return CacheStats(
            hits=self._hits,
            misses=self._misses,
            entries=len(self._cache),
            total_rows=total_rows
        )
