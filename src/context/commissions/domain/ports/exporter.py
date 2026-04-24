from abc import ABC, abstractmethod

import pandas as pd

from src.context.commissions.domain.value_objects import OutputConfig


class Exporter(ABC):

    @abstractmethod
    def export(
        self,
        data: pd.DataFrame,
        output_config: OutputConfig,
        plan_name: str = ""
    ) -> None:
        pass
