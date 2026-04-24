from abc import ABC, abstractmethod

from src.context.commissions.domain.ports.processing_strategy import ProcessingStrategy
from src.context.commissions.domain.value_objects import StrategyConfig


class StrategyFactory(ABC):

    @abstractmethod
    def create_strategy(self, config: StrategyConfig) -> ProcessingStrategy:
        pass
