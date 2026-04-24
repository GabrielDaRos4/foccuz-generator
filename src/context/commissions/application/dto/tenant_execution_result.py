from dataclasses import dataclass
from datetime import datetime

from .plan_execution_result import PlanExecutionResult


@dataclass
class TenantExecutionResult:
    tenant_id: str
    tenant_name: str
    total_plans: int
    successful_plans: int
    failed_plans: int
    plan_results: list[PlanExecutionResult]
    execution_time_seconds: float = 0.0
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.now())

    @property
    def success_rate(self) -> float:
        if self.total_plans == 0:
            return 0.0
        return (self.successful_plans / self.total_plans) * 100
