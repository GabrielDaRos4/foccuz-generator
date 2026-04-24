import logging

from .concat_merge_strategy import ConcatMergeStrategy
from .custom_merge_strategy import CustomMergeStrategy
from .data_merge_strategy import DataMergeStrategy
from .join_merge_strategy import JoinMergeStrategy

logger = logging.getLogger(__name__)


class DataMergeStrategyFactory:
    _strategies: dict[str, type[DataMergeStrategy]] = {}

    @classmethod
    def _initialize_strategies(cls) -> None:
        if not cls._strategies:
            cls._strategies['join'] = JoinMergeStrategy
            cls._strategies['concat'] = ConcatMergeStrategy

    @classmethod
    def create(cls, merge_type: str, **kwargs) -> DataMergeStrategy:
        cls._initialize_strategies()

        if merge_type == 'custom':
            merge_function = kwargs.get('merge_function')
            if not merge_function:
                raise ValueError(
                    "CustomMergeStrategy requires 'merge_function' argument"
                )
            return CustomMergeStrategy(merge_function)

        strategy_class = cls._strategies.get(merge_type)
        if not strategy_class:
            supported = list(cls._strategies.keys()) + ['custom']
            raise ValueError(
                f"Merge strategy '{merge_type}' not supported. "
                f"Supported types: {supported}"
            )

        return strategy_class()

    @classmethod
    def register_strategy(
        cls, name: str, strategy_class: type[DataMergeStrategy]
    ) -> None:
        if not issubclass(strategy_class, DataMergeStrategy):
            raise ValueError("strategy_class must extend DataMergeStrategy")

        cls._strategies[name] = strategy_class
        logger.info(f"Registered merge strategy: {name}")

    @classmethod
    def get_supported_types(cls) -> list[str]:
        cls._initialize_strategies()
        return list(cls._strategies.keys()) + ['custom']
