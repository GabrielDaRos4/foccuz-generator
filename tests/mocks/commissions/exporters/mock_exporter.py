import pandas as pd

from src.context.commissions.domain.ports import Exporter
from src.context.commissions.domain.value_objects import OutputConfig


class MockExporter(Exporter):

    def __init__(self):
        self.exported_data: list[tuple[pd.DataFrame, OutputConfig]] = []
        self.export_count = 0
        self.should_fail = False
        self.failure_message = "Export failed"

    def export(self, data: pd.DataFrame, config: OutputConfig) -> None:
        if self.should_fail:
            raise Exception(self.failure_message)

        self.exported_data.append((data.copy(), config))
        self.export_count += 1

    def get_last_export(self) -> tuple[pd.DataFrame, OutputConfig] | None:
        if self.exported_data:
            return self.exported_data[-1]
        return None

    def get_export_by_tab(self, tab_name: str) -> pd.DataFrame | None:
        for data, config in self.exported_data:
            if config.tab_name == tab_name:
                return data
        return None

    def reset(self) -> None:
        self.exported_data.clear()
        self.export_count = 0
        self.should_fail = False

    def set_failure(self, should_fail: bool = True, message: str = "Export failed") -> None:
        self.should_fail = should_fail
        self.failure_message = message
