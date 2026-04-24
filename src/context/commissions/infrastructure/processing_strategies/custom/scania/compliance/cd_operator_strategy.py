import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class CDOperatorStrategy(BaseScaniaStrategy):

    EXCLUDED_EMPLOYEE_IDS = [6010, 5411, 9359, 5200, 6224, 8831]

    COUNTRY_SALES_THRESHOLDS = [
        (110, float("inf"), 90000),
        (100, 109.99, 70000),
        (90, 99.99, 50000),
        (0, 89.99, 0),
    ]

    SERVICE_LEVEL_THRESHOLDS = [
        (100, float("inf"), 80000),
        (95, 99.99, 60000),
        (89, 89.99, 40000),
        (88, 88.99, 30000),
        (0, 87.99, 0),
    ]

    INVENTORY_ADJUSTMENT_THRESHOLDS = [
        (0, 1.49, 55000),
        (1.5, 3.0, 25000),
        (3.01, float("inf"), 0),
    ]

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        filtered_df, diagnostics = super()._filter_by_role_with_diagnostics(df)

        if self.EXCLUDED_EMPLOYEE_IDS:
            id_col = self._find_employee_id_column(filtered_df)
            if id_col:
                employee_ids = pd.to_numeric(filtered_df[id_col], errors="coerce")
                excluded_mask = employee_ids.isin(self.EXCLUDED_EMPLOYEE_IDS)
                excluded_count = excluded_mask.sum()
                if excluded_count > 0:
                    original_attrs = filtered_df.attrs.copy()
                    filtered_df = filtered_df[~excluded_mask].copy()
                    filtered_df.attrs = original_attrs
                    logger.info(f"Excluded {excluded_count} employees by ID: {self.EXCLUDED_EMPLOYEE_IDS}")

        return filtered_df, diagnostics

    def _find_employee_id_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "id" in col.lower() and "empleado" in col.lower():
                return col
        return None

    COLUMN_RENAME_MAP = {
        "cumplimiento venta pais": "Cumplimiento Venta País",
        "country_sales_payment": "Pago Cumplimiento Venta País",
        "nivel de servicio cd": "Nivel de Servicio CD",
        "service_level_payment": "Pago Nivel Servicio CD",
        "ajuste inventario": "Ajuste Inventario",
        "inventory_adjustment_payment": "Pago Ajuste Inventario",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Venta País", "Pago Cumplimiento Venta País",
        "Nivel de Servicio CD", "Pago Nivel Servicio CD",
        "Ajuste Inventario", "Pago Ajuste Inventario",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento Venta País": "percentage",
        "Pago Cumplimiento Venta País": "money",
        "Nivel de Servicio CD": "percentage",
        "Pago Nivel Servicio CD": "money",
        "Ajuste Inventario": "percentage",
        "Pago Ajuste Inventario": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._filter_employees_with_plan_data(result)
        if result.empty:
            return result

        result = self._calculate_country_sales_payment(result)
        result = self._calculate_service_level_payment(result)
        result = self._calculate_inventory_adjustment_payment(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _filter_employees_with_plan_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        sales_col = self._find_country_sales_column(result)
        service_col = self._find_service_level_column(result)
        inv_col = self._find_inventory_adjustment_column(result)

        plan_cols = [col for col in [sales_col, service_col, inv_col] if col]

        if not plan_cols:
            logger.warning("No plan data columns found")
            return pd.DataFrame()

        has_data = pd.Series(False, index=result.index)
        for col in plan_cols:
            col_values = pd.to_numeric(result[col], errors="coerce").fillna(0)
            has_data = has_data | (col_values != 0)

        result = result[has_data].copy()
        logger.info(f"Filtered to {len(result)} employees with plan data")
        return result

    def _calculate_country_sales_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        sales_col = self._find_country_sales_column(result)

        if sales_col:
            sales_pct = self._normalize_percentage(result[sales_col])
            result["cumplimiento venta pais"] = sales_pct
            result["country_sales_payment"] = sales_pct.apply(
                lambda x: self._get_threshold_payment(x * 100, self.COUNTRY_SALES_THRESHOLDS)
            )
        else:
            result["cumplimiento venta pais"] = 0
            result["country_sales_payment"] = 0

        return result

    def _find_country_sales_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "venta" in col_lower and ("pais" in col_lower or "nacional" in col_lower):
                return col
        return None

    def _calculate_service_level_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        service_col = self._find_service_level_column(result)

        if service_col:
            service_pct = self._normalize_percentage(result[service_col])
            result["nivel de servicio cd"] = service_pct
            result["service_level_payment"] = service_pct.apply(
                lambda x: self._get_threshold_payment(x * 100, self.SERVICE_LEVEL_THRESHOLDS)
            )
        else:
            result["nivel de servicio cd"] = 0
            result["service_level_payment"] = 0

        return result

    def _find_service_level_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "nivel" in col_lower and "servicio" in col_lower:
                return col
        return None

    def _calculate_inventory_adjustment_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        inv_col = self._find_inventory_adjustment_column(result)

        if inv_col:
            inv_values = pd.to_numeric(result[inv_col], errors="coerce").fillna(0)
            result["ajuste inventario"] = inv_values / 100
            result["inventory_adjustment_payment"] = inv_values.apply(
                lambda x: self._get_inventory_payment(x)
            )
        else:
            result["ajuste inventario"] = 0
            result["inventory_adjustment_payment"] = 0

        return result

    def _find_inventory_adjustment_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "ajuste" in col_lower and "inventario" in col_lower:
                return col
        return None

    def _get_inventory_payment(self, pct: float) -> int:
        if pd.isna(pct):
            return 0
        for lower, upper, payment in self.INVENTORY_ADJUSTMENT_THRESHOLDS:
            if lower <= pct <= upper:
                return payment
        return 0

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
            result["country_sales_payment"].fillna(0) +
            result["service_level_payment"].fillna(0) +
            result["inventory_adjustment_payment"].fillna(0)
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


OperarioCDStrategy = CDOperatorStrategy
