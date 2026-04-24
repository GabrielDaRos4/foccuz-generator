from dataclasses import dataclass
from datetime import datetime


@dataclass
class DomainEvent:
    occurred_at: datetime | None = None

    def __post_init__(self):
        if not self.occurred_at:
            object.__setattr__(self, 'occurred_at', datetime.now())
