from dataclasses import dataclass
from datetime import datetime

from src.context.shared.domain import DomainEvent


@dataclass
class CommissionExported(DomainEvent):
    tenant_id: str = ""
    plan_id: str = ""
    sheet_id: str = ""
    tab_name: str = ""
    records_count: int = 0
    occurred_at: datetime | None = None
