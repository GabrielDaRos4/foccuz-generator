from dataclasses import dataclass

from src.context.shared.domain import ValueObject

StrategyParamValue = str | int | float | bool | list | dict | None


@dataclass(frozen=True)
class StrategyConfig(ValueObject):
    module: str
    class_name: str
    params: dict[str, StrategyParamValue]

    def __post_init__(self):
        if not self.module:
            raise ValueError("Module cannot be empty")
        if not self.class_name:
            raise ValueError("Class name cannot be empty")
