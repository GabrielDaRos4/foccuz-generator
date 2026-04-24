from dataclasses import dataclass
from datetime import datetime

from dateutil.relativedelta import relativedelta

from .month_names import MONTH_NAMES_ES


@dataclass
class LicensePlateClassification:
    is_new: bool
    first_month_offset: int
    period: datetime = None

    @property
    def first_month_detail(self) -> str:
        if not self.period or self.first_month_offset is None:
            return ""
        first_month_date = self.period - relativedelta(months=self.first_month_offset)
        return f"{first_month_date.month} - {MONTH_NAMES_ES.get(first_month_date.month, '')}"
