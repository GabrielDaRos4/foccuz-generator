import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .bonus_calculator import BonusCalculator
from .bonus_config import BonusConfig
from .brand import Brand
from .brand_classifier import BrandClassifier
from .output_formatter import OutputFormatter
from .sales_filter import SalesFilter

logger = logging.getLogger(__name__)


class BrandBonusStrategy(ProcessingStrategy):

    def __init__(
        self,
        brand: str,
        min_sales: int,
        bonus_amount: float,
        target_period: str = None
    ):
        self._brand = Brand[brand.upper()]
        self._target_period = target_period
        self._classifier = BrandClassifier()
        self._filter = SalesFilter(self._classifier)
        self._calculator = BonusCalculator(BonusConfig(min_sales, bonus_amount))
        self._formatter = OutputFormatter()

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        data.columns = data.columns.str.lower().str.strip()
        year, month = self._get_period()

        filtered = self._filter.filter_by_brand(data, self._brand)
        if filtered.empty:
            return pd.DataFrame()

        filtered = self._filter.filter_delivered(filtered)
        if filtered.empty:
            return pd.DataFrame()

        filtered = self._filter.filter_by_period(filtered, year, month)
        if filtered.empty:
            return pd.DataFrame()

        bonuses = self._calculator.calculate(filtered)
        return self._formatter.format(bonuses, self._brand, datetime(year, month, 1))

    def _get_period(self) -> tuple[int, int]:
        if self._target_period:
            parts = self._target_period.split("-")
            return int(parts[0]), int(parts[1])
        now = datetime.now()
        return now.year, now.month
