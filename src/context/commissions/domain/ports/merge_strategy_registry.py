from abc import ABC, abstractmethod
from collections.abc import Callable

import pandas as pd


class MergeStrategyRegistry(ABC):

    @abstractmethod
    def get_merge_function(self, strategy_name: str) -> Callable[[dict[str, pd.DataFrame], dict], pd.DataFrame]:
        pass

    @abstractmethod
    def register(self, name: str, merge_function: Callable) -> None:
        pass
