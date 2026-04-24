from abc import ABC, abstractmethod

import pandas as pd


class ProcessingStrategy(ABC):
    @abstractmethod
    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
