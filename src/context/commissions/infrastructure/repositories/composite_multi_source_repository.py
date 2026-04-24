import logging

import pandas as pd

from src.context.commissions.domain.ports import MergeStrategyRegistry
from src.context.commissions.domain.repositories import (
    DataRepository,
    MultiSourceDataRepository,
)
from src.context.commissions.domain.value_objects import (
    CacheKey,
    DataSourceCollection,
    DataSourceConfig,
)
from src.context.commissions.infrastructure.cache import InMemoryDataSourceCache
from src.context.shared.domain.strategies import DataMergeStrategyFactory

logger = logging.getLogger(__name__)


class CompositeMultiSourceDataRepository(MultiSourceDataRepository):
    def __init__(
        self,
        source_repositories: dict[str, DataRepository],
        merge_registry: MergeStrategyRegistry
    ):
        self._source_repositories = source_repositories
        self._merge_registry = merge_registry
        self._cache = InMemoryDataSourceCache()
        logger.info(
            f"Initialized CompositeMultiSourceDataRepository with "
            f"{len(source_repositories)} source types and caching enabled"
        )

    def start_cache_session(self, session_id: str = None) -> None:
        self._cache.clear()
        logger.info(f"Cache session started: {session_id or 'default'}")

    def end_cache_session(self) -> dict:
        stats = self._cache.get_stats()
        self._cache.clear()
        logger.info(
            f"Cache session ended - Hits: {stats.hits}, Misses: {stats.misses}, "
            f"Hit Rate: {stats.hit_rate:.1%}"
        )
        return {
            'hits': stats.hits,
            'misses': stats.misses,
            'entries': stats.entries,
            'hit_rate': stats.hit_rate
        }

    def register_repository(self, source_type: str, repository: DataRepository):
        self._source_repositories[source_type] = repository
        logger.info(f"Registered repository for source type: {source_type}")

    def fetch_single_source(self, source: DataSourceConfig) -> pd.DataFrame:
        logger.info(f"Fetching data from single source: {source.source_id} (type: {source.source_type})")

        cache_key = CacheKey.from_data_source(source.source_type, source.config)

        cached_data = self._cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        repo = self._get_repository(source.source_type)

        try:
            data = repo.get_data_for_plan(source)

            self._cache.set(cache_key, data)
            logger.info(f"Fetched {len(data)} rows from {source.source_id}")
            return data

        except Exception as e:
            logger.error(f"Error fetching from {source.source_id}: {str(e)}")
            raise

    def fetch_multiple_sources(
        self,
        collection: DataSourceCollection
    ) -> dict[str, pd.DataFrame]:
        logger.info(f"Fetching data from {len(collection.sources)} sources")

        results = {}
        errors = {}

        for source in collection.sources:
            try:
                cache_key = CacheKey.from_data_source(source.source_type, source.config)

                cached_data = self._cache.get(cache_key)
                if cached_data is not None:
                    if isinstance(cached_data, dict):
                        results.update(cached_data)
                    else:
                        results[source.source_id] = cached_data
                    continue

                repo = self._get_repository(source.source_type)
                data = repo.get_data_for_plan(source)

                if isinstance(data, dict):
                    results.update(data)
                    logger.info(f"{source.source_id}: Loaded {len(data)} sub-sources")
                    for key, df in data.items():
                        sub_cache_key = CacheKey.from_data_source(f"{source.source_type}_{key}", source.config)
                        self._cache.set(sub_cache_key, df)
                else:
                    results[source.source_id] = data
                    logger.info(f"{source.source_id}: {len(data)} rows, {len(data.columns)} columns")
                    self._cache.set(cache_key, data)

            except Exception as e:
                error_msg = f"Error fetching {source.source_id}: {str(e)}"
                logger.error(f"{error_msg}")
                errors[source.source_id] = str(e)

        if errors:
            logger.warning(f"Errors occurred: {errors}")

        logger.info(f"Successfully fetched {len(results)} total data sources")
        return results

    def merge_sources(
        self,
        dataframes: dict[str, pd.DataFrame],
        collection: DataSourceCollection,
        plan_params: dict = None
    ) -> pd.DataFrame:
        if not collection.merge_strategy:
            primary = collection.get_primary_source()
            logger.info(f"No merge strategy specified, returning primary source: {primary.source_id}")
            return dataframes[primary.source_id]

        logger.info(
            f"Merging {len(dataframes)} sources using strategy: "
            f"{collection.merge_strategy.merge_type}"
        )

        try:
            if collection.merge_strategy.merge_type == 'custom':
                strategy_name = collection.merge_strategy.merge_config.get('strategy_name')
                if strategy_name:
                    merge_function = self._merge_registry.get_merge_function(strategy_name)
                    merge_strategy = DataMergeStrategyFactory.create(
                        'custom',
                        merge_function=merge_function
                    )
                else:
                    raise ValueError("Custom merge strategy requires 'strategy_name' in config")
            else:
                merge_strategy = DataMergeStrategyFactory.create(
                    collection.merge_strategy.merge_type
                )

            merge_config = collection.merge_strategy.merge_config.copy()
            if plan_params:
                merge_config.update(plan_params)

            result = merge_strategy.merge(
                dataframes,
                merge_config
            )

            logger.info(
                f"Merge completed successfully: {len(result)} rows, "
                f"{len(result.columns)} columns"
            )

            return result

        except Exception as e:
            logger.error(f"Merge failed: {str(e)}")
            raise

    def _get_repository(self, source_type: str) -> DataRepository:
        repo = self._source_repositories.get(source_type)
        if not repo:
            supported = list(self._source_repositories.keys())
            raise ValueError(
                f"Unsupported data source type: '{source_type}'. "
                f"Supported types: {supported}"
            )
        return repo

    def get_supported_source_types(self) -> list:
        return list(self._source_repositories.keys())
