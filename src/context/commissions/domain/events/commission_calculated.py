from dataclasses import dataclass
from datetime import datetime

from src.context.shared.domain import DomainEvent


@dataclass
class CommissionCalculated(DomainEvent):
    tenant_id: str = ""
    plan_id: str = ""
    records_count: int = 0
    total_commission: float = 0.0
    occurred_at: datetime | None = None
