from dataclasses import dataclass
from datetime import datetime


@dataclass
class PlanExecutionResult:
    plan_id: str
    plan_name: str
    success: bool
    records_processed: int
    total_commission: float
    error_message: str | None = None
    warning_message: str | None = None
    execution_time_seconds: float = 0.0
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.now())
