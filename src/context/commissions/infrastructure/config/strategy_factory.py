import importlib
import logging

from src.context.commissions.domain.ports import ProcessingStrategy, StrategyFactory
from src.context.commissions.domain.value_objects import StrategyConfig

logger = logging.getLogger(__name__)


class DynamicStrategyFactory(StrategyFactory):
    def __init__(self):
        self._cache: dict[str, type] = {}

    def create_strategy(self, config: StrategyConfig) -> ProcessingStrategy:
        cache_key = f"{config.module}.{config.class_name}"

        if cache_key not in self._cache:
            strategy_class = self._load_strategy_class(config.module, config.class_name)
            self._cache[cache_key] = strategy_class
        else:
            strategy_class = self._cache[cache_key]

        try:
            logger.info(f"Creating strategy {cache_key} with params: {config.params}")
            instance = strategy_class(**config.params)
            logger.info(f"Created strategy instance: {cache_key}")
            return instance
        except Exception as e:
            logger.error(f"Error instantiating strategy {cache_key}: {str(e)}")
            raise

    @staticmethod
    def _load_strategy_class(module_path: str, class_name: str) -> type:
        try:
            module = importlib.import_module(module_path)
            strategy_class = getattr(module, class_name)

            if not issubclass(strategy_class, ProcessingStrategy):
                raise TypeError(
                    f"{class_name} is not a subclass of ProcessingStrategy"
                )

            return strategy_class

        except ImportError as e:
            logger.error(f"Cannot import module {module_path}: {str(e)}")
            raise
        except AttributeError as e:
            logger.error(f"Class {class_name} not found in module {module_path}: {str(e)}")
            raise

    def clear_cache(self) -> None:
        self._cache.clear()
