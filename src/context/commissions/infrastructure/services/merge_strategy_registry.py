from collections.abc import Callable

import pandas as pd

from src.context.commissions.domain.ports import (
    MergeStrategyRegistry as MergeStrategyRegistryPort,
)


class InMemoryMergeStrategyRegistry(MergeStrategyRegistryPort):
    def __init__(self):
        self._strategies: dict[str, Callable[[dict[str, pd.DataFrame], dict], pd.DataFrame]] = {}

    def get_merge_function(self, strategy_name: str) -> Callable[[dict[str, pd.DataFrame], dict], pd.DataFrame]:
        func = self._strategies.get(strategy_name)
        if not func:
            raise ValueError(f"Unknown merge strategy: {strategy_name}")
        return func

    def register(self, name: str, merge_function: Callable) -> None:
        self._strategies[name] = merge_function

    def get_registered_strategies(self) -> list:
        return list(self._strategies.keys())
