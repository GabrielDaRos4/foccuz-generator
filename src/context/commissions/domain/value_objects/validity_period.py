from dataclasses import dataclass
from datetime import datetime

from src.context.shared.domain import ValueObject


@dataclass(frozen=True)
class ValidityPeriod(ValueObject):
    valid_from: datetime | None = None
    valid_until: datetime | None = None

    def is_valid_at(self, date: datetime) -> bool:
        if self.valid_from and date < self.valid_from:
            return False
        if self.valid_until and date > self.valid_until:
            return False
        return True

    def is_currently_valid(self, now: datetime | None = None) -> bool:
        return self.is_valid_at(now or datetime.now())
