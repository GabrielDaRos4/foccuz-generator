import logging
from collections.abc import Callable

import pandas as pd

from .data_merge_strategy import DataMergeStrategy

logger = logging.getLogger(__name__)

MergeConfig = dict[str, str | int | float | bool | list | dict]
MergeFunctionType = Callable[[dict[str, pd.DataFrame], MergeConfig], pd.DataFrame]


class CustomMergeStrategy(DataMergeStrategy):
    def __init__(self, merge_function: MergeFunctionType):
        if not callable(merge_function):
            raise ValueError("merge_function must be callable")
        self.merge_function = merge_function

    def merge(
        self,
        dataframes: dict[str, pd.DataFrame],
        config: dict[str, str | int | float | bool | list | dict]
    ) -> pd.DataFrame:
        logger.info(f"Using custom merge function: {self.merge_function.__name__}")
        result = self.merge_function(dataframes, config)

        if not isinstance(result, pd.DataFrame):
            raise ValueError("Custom merge function must return a pandas DataFrame")

        logger.info(f"Custom merge completed: {len(result)} rows")
        return result
