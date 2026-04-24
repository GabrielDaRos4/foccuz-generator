from dataclasses import dataclass, field
from datetime import datetime

from src.context.shared.domain import DomainEvent


@dataclass
class PlanProcessingFailed(DomainEvent):
    tenant_id: str = ""
    plan_id: str = ""
    error_message: str = ""
    error_details: dict[str, str] = field(default_factory=dict)
    occurred_at: datetime | None = None
