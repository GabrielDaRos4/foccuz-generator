from abc import ABC, abstractmethod

import pandas as pd

from src.context.commissions.domain.value_objects import DataSourceConfig


class DataRepository(ABC):

    @abstractmethod
    def get_data_for_plan(self, source: DataSourceConfig) -> pd.DataFrame:
        pass

