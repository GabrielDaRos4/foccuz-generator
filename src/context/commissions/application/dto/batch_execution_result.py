from dataclasses import dataclass
from datetime import datetime

from .tenant_execution_result import TenantExecutionResult


@dataclass
class BatchExecutionResult:
    total_tenants: int
    successful_tenants: int
    failed_tenants: int
    total_plans: int
    successful_plans: int
    failed_plans: int
    tenant_results: list[TenantExecutionResult]
    execution_time_seconds: float = 0.0
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.now())

    @property
    def tenant_success_rate(self) -> float:
        if self.total_tenants == 0:
            return 0.0
        return (self.successful_tenants / self.total_tenants) * 100

    @property
    def plan_success_rate(self) -> float:
        if self.total_plans == 0:
            return 0.0
        return (self.successful_plans / self.total_plans) * 100
