from dataclasses import dataclass

from src.context.shared.domain import ValueObject


@dataclass(frozen=True)
class OutputConfig(ValueObject):
    sheet_id: str
    tab_name: str
    clear_before_write: bool = True

    def __post_init__(self):
        if not self.sheet_id:
            raise ValueError("Sheet ID cannot be empty")
        if not self.tab_name:
            raise ValueError("Tab name cannot be empty")
