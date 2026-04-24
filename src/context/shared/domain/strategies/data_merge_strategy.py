from abc import ABC, abstractmethod

import pandas as pd


class DataMergeStrategy(ABC):

    @abstractmethod
    def merge(
        self,
        dataframes: dict[str, pd.DataFrame],
        config: dict[str, str | int | float | bool | list | dict]
    ) -> pd.DataFrame:
        pass
