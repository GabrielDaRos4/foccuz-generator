import pandas as pd

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig


class MockDataRepository(DataRepository):

    def __init__(self, data: pd.DataFrame = None):
        self._data = data if data is not None else pd.DataFrame()
        self._call_count = 0
        self._last_config: DataSourceConfig | None = None
        self.should_fail = False
        self.failure_message = "Data fetch failed"

    def get_data(self, config: DataSourceConfig) -> pd.DataFrame:
        if self.should_fail:
            raise Exception(self.failure_message)

        self._call_count += 1
        self._last_config = config
        return self._data.copy()

    def set_data(self, data: pd.DataFrame) -> None:
        self._data = data

    @property
    def call_count(self) -> int:
        return self._call_count

    @property
    def last_config(self) -> DataSourceConfig | None:
        return self._last_config

    def reset(self) -> None:
        self._call_count = 0
        self._last_config = None
        self.should_fail = False

    def set_failure(self, should_fail: bool = True, message: str = "Data fetch failed") -> None:
        self.should_fail = should_fail
        self.failure_message = message
