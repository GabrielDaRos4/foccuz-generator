from dataclasses import dataclass

from src.context.shared.domain import ValueObject


@dataclass(frozen=True)
class DataMergeConfig(ValueObject):
    merge_type: str
    primary_source_id: str
    merge_config: dict[str, str | int | float | bool | list | dict]

    def __post_init__(self):
        if not self.merge_type:
            raise ValueError("merge_type cannot be empty")
        if not self.primary_source_id:
            raise ValueError("primary_source_id cannot be empty")
        if not isinstance(self.merge_config, dict):
            raise ValueError("merge_config must be a dictionary")
