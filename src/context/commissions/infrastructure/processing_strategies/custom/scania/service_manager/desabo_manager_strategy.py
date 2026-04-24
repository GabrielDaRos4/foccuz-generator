import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class DesaboManagerStrategy(BaseScaniaStrategy):

    SALES_COMPLIANCE_THRESHOLDS = [
        (110, float("inf"), 680000),
        (105, 109.99, 580000),
        (100, 104.99, 480000),
        (95, 99.99, 380000),
        (90, 94.99, 280000),
        (0, 89.99, 0),
    ]

    PRODUCTIVITY_THRESHOLDS = [
        (100, float("inf"), 200000),
        (95, 99.99, 180000),
        (90, 94.99, 160000),
        (85, 89.99, 140000),
        (80, 84.99, 120000),
        (0, 79.99, 0),
    ]

    EBIT_THRESHOLDS = [
        (26, float("inf"), 160000),
        (21, 25.99, 140000),
        (16, 20.99, 120000),
        (11, 15.99, 100000),
        (6, 10.99, 80000),
        (0, 5.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "cumplimiento venta": "Cumplimiento Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "cumplimiento productividad": "Cumplimiento Productividad",
        "productivity_payment": "Pago Productividad",
        "ebit": "Ebit",
        "ebit_payment": "Pago EBIT",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Cumplimiento Productividad", "Pago Productividad",
        "Ebit", "Pago EBIT",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Cumplimiento Productividad": "percentage",
        "Pago Productividad": "money",
        "Ebit": "percentage",
        "Pago EBIT": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._calculate_sales_compliance_payment(result)
        result = self._calculate_productivity_payment(result)
        result = self._calculate_ebit_payment(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _calculate_sales_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        compliance_col = self._find_sales_compliance_column(result)

        if compliance_col:
            compliance = self._normalize_percentage(result[compliance_col])
            result["cumplimiento venta"] = compliance
            result["sales_compliance_payment"] = compliance.apply(
                lambda x: self._get_threshold_payment(x * 100, self.SALES_COMPLIANCE_THRESHOLDS)
            )
        else:
            result["cumplimiento venta"] = 0
            result["sales_compliance_payment"] = 0

        return result

    def _find_sales_compliance_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "cumplimiento" in col_lower and "venta" in col_lower:
                return col
        for col in df.columns:
            col_lower = col.lower()
            if "cumplimiento" in col_lower or col_lower == "% cumplimiento":
                return col
        return None

    def _calculate_productivity_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        productivity_col = self._find_productivity_column(result)

        if productivity_col:
            productivity = self._normalize_percentage(result[productivity_col])
            result["cumplimiento productividad"] = productivity
            result["productivity_payment"] = productivity.apply(
                lambda x: self._get_threshold_payment(x * 100, self.PRODUCTIVITY_THRESHOLDS)
            )
        else:
            result["cumplimiento productividad"] = 0
            result["productivity_payment"] = 0

        return result

    def _find_productivity_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "productividad" in col.lower():
                return col
        return None

    def _calculate_ebit_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        ebit_col = self._find_ebit_column(result)

        if ebit_col:
            ebit = self._normalize_percentage(result[ebit_col])
            result["ebit"] = ebit
            result["ebit_payment"] = ebit.apply(
                lambda x: self._get_threshold_payment(x * 100, self.EBIT_THRESHOLDS)
            )
        else:
            result["ebit"] = 0
            result["ebit_payment"] = 0

        return result

    def _find_ebit_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "ebit" in col.lower():
                return col
        return None

    def _normalize_percentage(self, series: pd.Series) -> pd.Series:
        values = pd.to_numeric(series, errors="coerce").fillna(0)
        if values.max() > 2:
            values = values / 100
        return values

    def _get_threshold_payment(self, pct: float, thresholds: list) -> int:
        if pd.isna(pct):
            return 0
        for lower, upper, payment in thresholds:
            if lower <= pct <= upper:
                return payment
        return 0

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["commission"] = (
            result["sales_compliance_payment"].fillna(0) +
            result["productivity_payment"].fillna(0) +
            result["ebit_payment"].fillna(0)
        )
        result = self._apply_days_proration(result, "commission")
        return result

    def _apply_guaranteed_minimum(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        guaranteed_col = self._find_guaranteed_column(result)

        if guaranteed_col:
            result["guaranteed"] = pd.to_numeric(
                result[guaranteed_col].replace("NA", 0), errors="coerce"
            ).fillna(0)
        else:
            result["guaranteed"] = 0

        result["commission"] = result[["commission", "guaranteed"]].max(axis=1)
        return result

    def _find_guaranteed_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "garantizado" in col.lower():
                return col
        return None


JefeDesaboStrategy = DesaboManagerStrategy
