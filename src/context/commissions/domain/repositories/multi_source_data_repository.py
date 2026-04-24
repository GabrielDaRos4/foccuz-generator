from abc import ABC, abstractmethod

import pandas as pd

from src.context.commissions.domain.aggregates import Plan
from src.context.commissions.domain.value_objects import (
    DataSourceCollection,
    DataSourceConfig,
)


class MultiSourceDataRepository(ABC):
    @abstractmethod
    def fetch_single_source(self, source: DataSourceConfig) -> pd.DataFrame:
        pass

    @abstractmethod
    def fetch_multiple_sources(
        self,
        collection: DataSourceCollection
    ) -> dict[str, pd.DataFrame]:
        pass

    @abstractmethod
    def merge_sources(
        self,
        dataframes: dict[str, pd.DataFrame],
        collection: DataSourceCollection,
        plan_params: dict | None = None
    ) -> pd.DataFrame:
        pass

    def get_data_for_plan(self, plan: Plan) -> pd.DataFrame:
        collection = plan.data_sources

        if collection.is_single_source():
            return self.fetch_single_source(collection.sources[0])

        dataframes = self.fetch_multiple_sources(collection)

        plan_params = plan.strategy_config.params if plan.strategy_config else {}

        return self.merge_sources(dataframes, collection, plan_params)

