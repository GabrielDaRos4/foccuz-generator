import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class SalesCommissionStrategy(BaseScaniaStrategy):

    COLUMN_RENAME_MAP = {
        "total_sales": "Venta Total",
        "commission_sum": "Comisión Ventas",
        "sale_count": "Cantidad Ventas",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta Total", "Comisión Ventas", "Cantidad Ventas",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta Total": "money",
        "Comisión Ventas": "money",
        "Cantidad Ventas": "integer",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_sales_data(result)
        result = self._calculate_commission_from_sales(result)
        result = self._apply_guaranteed_minimum(result)

        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        sales_col = self._find_sales_amount_column(result)
        if sales_col:
            result["total_sales"] = pd.to_numeric(
                result[sales_col], errors="coerce"
            ).fillna(0)
        else:
            result["total_sales"] = 0

        commission_col = self._find_commission_column(result)
        if commission_col:
            result["commission_sum"] = pd.to_numeric(
                result[commission_col], errors="coerce"
            ).fillna(0)
        else:
            result["commission_sum"] = 0

        result["sale_count"] = 1

        return result

    def _find_sales_amount_column(self, df: pd.DataFrame) -> str | None:
        patterns = ["valor $", "valor_$", "precio venta", "precio_venta", "monto"]
        for pattern in patterns:
            for col in df.columns:
                if pattern in col.lower():
                    return col
        return None

    def _find_commission_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if col_lower == "comision" or col_lower == "comisión":
                if self._column_has_numeric_values(df, col):
                    return col

        patterns = ["comision vendedor", "comision_vendedor", "comisión vendedor"]
        for pattern in patterns:
            for col in df.columns:
                if pattern in col.lower():
                    if self._column_has_numeric_values(df, col):
                        return col

        for col in df.columns:
            col_lower = col.lower()
            if "comision" in col_lower or "comisión" in col_lower:
                if "cliente" not in col_lower:
                    if self._column_has_numeric_values(df, col):
                        return col

        return None

    def _column_has_numeric_values(self, df: pd.DataFrame, col: str) -> bool:
        values = pd.to_numeric(df[col], errors="coerce")
        return values.notna().any() and values.sum() > 0

    def _calculate_commission_from_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        commission_values = result["commission_sum"]
        if commission_values.sum() > 0:
            if commission_values.max() <= 1:
                result["commission"] = result["total_sales"] * commission_values
            else:
                result["commission"] = commission_values
        else:
            result["commission"] = result["total_sales"] * 0.01

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
