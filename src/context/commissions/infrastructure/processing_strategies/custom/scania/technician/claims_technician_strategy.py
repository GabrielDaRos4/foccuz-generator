import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ClaimsTechnicianStrategy(BaseScaniaStrategy):

    SALES_COMPLIANCE_THRESHOLDS = [
        (110, float("inf"), 330000),
        (105, 109.99, 230000),
        (100, 104.99, 130000),
        (95, 99.99, 80000),
        (90, 94.99, 30000),
        (0, 89.99, 0),
    ]

    PRODUCTIVITY_THRESHOLDS = [
        (100, float("inf"), 150000),
        (95, 99.99, 130000),
        (90, 94.99, 110000),
        (85, 89.99, 90000),
        (80, 84.99, 70000),
        (0, 79.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "cumplimiento venta": "Cumplimiento Venta",
        "resultado venta": "Resultado Venta",
        "meta venta": "Meta Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "cumplimiento productividad": "Cumplimiento Productividad",
        "productivity_payment": "Pago Productividad",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Venta", "Resultado Venta", "Meta Venta",
        "Pago Cumplimiento Venta", "Cumplimiento Productividad", "Pago Productividad",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento Venta": "percentage",
        "Resultado Venta": "money",
        "Meta Venta": "money",
        "Pago Cumplimiento Venta": "money",
        "Cumplimiento Productividad": "percentage",
        "Pago Productividad": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._extract_sales_data(result)
        result = self._calculate_sales_compliance_payment(result)
        result = self._calculate_productivity_payment(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result_col = self._find_result_column(result)
        if result_col:
            result["resultado venta"] = pd.to_numeric(result[result_col], errors="coerce").fillna(0)
        else:
            result["resultado venta"] = 0

        target_col = self._find_target_column(result)
        if target_col:
            result["meta venta"] = pd.to_numeric(result[target_col], errors="coerce").fillna(0)
        else:
            result["meta venta"] = 0

        compliance_col = self._find_sales_compliance_column(result)
        if compliance_col:
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["cumplimiento venta"] = compliance
        else:
            result["cumplimiento venta"] = 0

        return result

    def _find_result_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "resultado" in col_lower or "actual" in col_lower:
                return col
        return None

    def _find_target_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "meta" in col_lower or "budget" in col_lower:
                return col
        return None

    def _find_sales_compliance_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "cumplimiento" in col_lower and "venta" in col_lower:
                return col
        return None

    def _calculate_sales_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["sales_compliance_payment"] = result["cumplimiento venta"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.SALES_COMPLIANCE_THRESHOLDS)
        )
        return result

    def _calculate_productivity_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        productivity_col = self._find_productivity_column(result)

        if productivity_col:
            productivity = pd.to_numeric(result[productivity_col], errors="coerce").fillna(0)
            if productivity.max() > 2:
                productivity = productivity / 100
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
            result["productivity_payment"].fillna(0)
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


TecnicoSiniestrosStrategy = ClaimsTechnicianStrategy
