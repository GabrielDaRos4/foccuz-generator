import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class CDAdminStrategy(BaseScaniaStrategy):

    COUNTRY_SALES_THRESHOLDS = [
        (110, float("inf"), 170000),
        (100, 109.99, 140000),
        (90, 99.99, 110000),
        (0, 89.99, 0),
    ]

    SERVICE_LEVEL_THRESHOLDS = [
        (100, float("inf"), 130000),
        (95, 99.99, 110000),
        (89, 89.99, 90000),
        (88, 88.99, 70000),
        (0, 87.99, 0),
    ]

    INVENTORY_ADJUSTMENT_THRESHOLDS = [
        (0, 1.49, 100000),
        (1.5, 3.0, 60000),
        (3.01, float("inf"), 0),
    ]

    def __init__(self, employee_ids: list[int] = None, **kwargs):
        super().__init__(**kwargs)
        self._employee_ids = [str(eid) for eid in (employee_ids or [])]

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': self._role_filter,
            'employee_ids': self._employee_ids,
            'filtered_out_by_role': 0,
        }

        if self._employee_ids:
            id_col = next(
                (c for c in df.columns if 'id' in c.lower() and 'empleado' in c.lower()),
                None
            )
            if id_col:
                df_ids = df[id_col].astype(str).str.strip()
                mask = df_ids.isin(self._employee_ids)
                result = df[mask].copy()
                result.attrs = df.attrs.copy()
                diagnostics['matched_by_id'] = len(result)
                diagnostics['filtered_out_by_role'] = len(df) - len(result)
                logger.info(f"Filtered by employee IDs: {len(result)} of {len(df)}")
                return result, diagnostics

        return super()._filter_by_role_with_diagnostics(df)

    COLUMN_RENAME_MAP = {
        "cumplimiento venta pais": "Cumplimiento Venta País",
        "country_sales_payment": "Pago Cumplimiento Venta País",
        "nivel de servicio cd": "Nivel de Servicio CD",
        "service_level_payment": "Pago Nivel de Servicio CD",
        "ajuste inventario": "Ajuste Inventario",
        "inventory_adjustment_payment": "Pago Ajuste Inventario",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Venta País", "Pago Cumplimiento Venta País",
        "Nivel de Servicio CD", "Pago Nivel de Servicio CD",
        "Ajuste Inventario", "Pago Ajuste Inventario",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento Venta País": "percentage",
        "Pago Cumplimiento Venta País": "money",
        "Nivel de Servicio CD": "percentage",
        "Pago Nivel de Servicio CD": "money",
        "Ajuste Inventario": "percentage",
        "Pago Ajuste Inventario": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._calculate_country_sales_payment(result)
        result = self._calculate_service_level_payment(result)
        result = self._calculate_inventory_adjustment_payment(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
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
                lambda x: self._get_service_level_payment(x * 100)
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

    def _get_service_level_payment(self, pct: float) -> int:
        if pd.isna(pct):
            return 0
        for lower, upper, payment in self.SERVICE_LEVEL_THRESHOLDS:
            if lower <= pct <= upper:
                return payment
        return 0

    def _calculate_inventory_adjustment_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        inv_col = self._find_inventory_adjustment_column(result)

        if inv_col:
            inv_value = pd.to_numeric(result[inv_col], errors="coerce").fillna(0)
            result["ajuste inventario"] = inv_value / 100
            result["inventory_adjustment_payment"] = inv_value.apply(
                lambda x: self._get_inventory_adjustment_payment(x)
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

    def _get_inventory_adjustment_payment(self, pct: float) -> int:
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


AdministrativoCDStrategy = CDAdminStrategy
