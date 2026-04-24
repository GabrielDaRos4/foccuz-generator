from dataclasses import dataclass, field

from src.context.commissions.domain.exceptions import InvalidPlanError
from src.context.commissions.domain.value_objects import (
    DataSourceCollection,
    OutputConfig,
    StrategyConfig,
    ValidityPeriod,
)


@dataclass
class Plan:
    id: str
    name: str
    tenant_id: str
    active: bool
    data_sources: DataSourceCollection
    output_config: OutputConfig
    strategy_config: StrategyConfig
    validity_period: ValidityPeriod = field(default_factory=ValidityPeriod)
    depends_on: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.id:
            raise InvalidPlanError("Plan ID cannot be empty")
        if not self.name:
            raise InvalidPlanError("Plan name cannot be empty")
        if not self.tenant_id:
            raise InvalidPlanError("Tenant ID cannot be empty")

    @property
    def full_id(self) -> str:
        return f"{self.tenant_id}.{self.id}"

    def is_executable(self) -> bool:
        return self.active and self.validity_period.is_currently_valid()

    def requires_multiple_sources(self) -> bool:
        return not self.data_sources.is_single_source()

    def deactivate(self) -> None:
        self.active = False

    def activate(self) -> None:
        self.active = True
