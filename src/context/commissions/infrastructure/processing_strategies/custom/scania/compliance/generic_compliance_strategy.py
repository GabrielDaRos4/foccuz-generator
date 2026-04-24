import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class GenericComplianceStrategy(BaseScaniaStrategy):

    DEFAULT_THRESHOLDS = [
        (110, float("inf"), 300000),
        (100, 109.99, 200000),
        (90, 99.99, 100000),
        (0, 89.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "compliance": "Cumplimiento",
        "commission_payment": "Pago Cumplimiento",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento", "Pago Cumplimiento",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento": "percentage",
        "Pago Cumplimiento": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def __init__(
        self,
        role_filter: list[str] = None,
        thresholds: list[tuple] = None,
        compliance_column: str = None,
        **kwargs,
    ):
        super().__init__(role_filter, **kwargs)
        self._thresholds = thresholds or self.DEFAULT_THRESHOLDS
        self._compliance_column = compliance_column

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance_col = self._find_compliance_column(result)
        if compliance_col:
            result["compliance"] = pd.to_numeric(
                result[compliance_col], errors="coerce"
            ).fillna(0)
            if result["compliance"].max() > 2:
                result["compliance"] = result["compliance"] / 100
        else:
            result["compliance"] = self._calculate_compliance_from_sales(result)

        result["commission_payment"] = result["compliance"].apply(
            lambda x: self.calculate_compliance_payment(x, self._thresholds)
        )

        result = self._apply_days_proration(result, "commission_payment")

        return result

    def _find_compliance_column(self, df: pd.DataFrame) -> str | None:
        if self._compliance_column and self._compliance_column in df.columns:
            return self._compliance_column

        for pattern in ["cumplimiento", "cumpl", "compliance"]:
            col = next((c for c in df.columns if pattern in c.lower()), None)
            if col:
                return col

        return None

    def _calculate_compliance_from_sales(self, df: pd.DataFrame) -> pd.Series:
        sales_col = next(
            (c for c in df.columns if "venta" in c or "actual" in c), None
        )
        target_col = next(
            (c for c in df.columns if "meta" in c or "budget" in c), None
        )

        if sales_col and target_col:
            sales = pd.to_numeric(df[sales_col], errors="coerce").fillna(0)
            target = pd.to_numeric(df[target_col], errors="coerce").fillna(1)
            return sales / target.replace(0, 1)

        return pd.Series([0] * len(df), index=df.index)
