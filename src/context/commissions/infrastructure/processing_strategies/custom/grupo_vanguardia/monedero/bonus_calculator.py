import logging

import pandas as pd

from .bonus_config import BonusConfig
from .consultant_bonus import ConsultantBonus

logger = logging.getLogger(__name__)


class BonusCalculator:

    def __init__(self, config: BonusConfig):
        self._config = config

    def calculate(self, sales_df: pd.DataFrame) -> list[ConsultantBonus]:
        sales_df = sales_df.copy()
        sales_df.columns = sales_df.columns.str.lower().str.strip()

        consultant_id_col = self._find_consultant_id_col(sales_df.columns)
        if not consultant_id_col:
            raise ValueError("No consultant ID column found")

        name_col = self._find_column(sales_df.columns, "consultant", "name")
        mail_col = self._find_column(sales_df.columns, "consultant", "mail")
        agency_col = self._find_column(sales_df.columns, "agency")

        results = []
        for consultant_id, group in sales_df.groupby(consultant_id_col):
            sales_count = len(group)
            qualifies = sales_count >= self._config.min_sales
            first_row = group.iloc[0]

            results.append(ConsultantBonus(
                consultant_id=str(consultant_id),
                consultant_name=str(first_row[name_col]) if name_col else "",
                consultant_email=str(first_row[mail_col]) if mail_col else "",
                agency=str(first_row[agency_col]) if agency_col else "",
                sales_count=sales_count,
                target=self._config.min_sales,
                qualifies=qualifies,
                bonus=self._config.bonus_amount if qualifies else 0
            ))

        qualified = sum(1 for r in results if r.qualifies)
        total_bonus = sum(r.bonus for r in results)
        logger.info(f"Calculated: {len(results)} consultants, {qualified} qualified, ${total_bonus:,.0f}")

        return results

    @staticmethod
    def _find_consultant_id_col(columns: pd.Index) -> str | None:
        for col in columns:
            if "idconsultant" in col.replace("_", "").replace(" ", ""):
                return col
        return None

    @staticmethod
    def _find_column(columns: pd.Index, *keywords: str) -> str | None:
        for col in columns:
            col_lower = col.lower()
            if all(k in col_lower for k in keywords):
                return col
        return None
