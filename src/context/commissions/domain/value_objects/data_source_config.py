from dataclasses import dataclass

from src.context.shared.domain import ValueObject


@dataclass(frozen=True)
class DataSourceConfig(ValueObject):
    source_id: str
    source_type: str
    config: dict[str, str | int | float | bool | list | dict]

    def __post_init__(self):
        if not self.source_id:
            raise ValueError("source_id cannot be empty")
        if not self.source_type:
            raise ValueError("source_type cannot be empty")
        if not isinstance(self.config, dict):
            raise ValueError("config must be a dictionary")
