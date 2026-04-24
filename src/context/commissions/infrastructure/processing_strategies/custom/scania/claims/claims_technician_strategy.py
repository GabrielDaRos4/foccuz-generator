import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ClaimsTechnicianStrategy(BaseScaniaStrategy):

    def __init__(
        self,
        role_filter: list[str] = None,
        target_period: str = None,
        employee_ids: list[str] = None,
        **kwargs
    ):
        super().__init__(role_filter=role_filter, target_period=target_period, **kwargs)
        self._employee_ids = employee_ids or []

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
        "resultado venta": "Resultado Venta",
        "meta venta": "Meta Venta",
        "cumplimiento venta": "Cumplimiento Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "cumplimiento productividad": "Cumplimiento Productividad",
        "productivity_payment": "Pago Productividad",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Venta", "Resultado Venta", "Meta Venta",
        "Pago Cumplimiento Venta",
        "Cumplimiento Productividad", "Pago Productividad",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Resultado Venta": "money",
        "Meta Venta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Cumplimiento Productividad": "percentage",
        "Pago Productividad": "money",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': self._role_filter,
            'employee_ids_filter': self._employee_ids,
            'filtered_out_by_role': 0,
        }

        result = df.copy()
        result.attrs = df.attrs.copy()

        if self._employee_ids:
            id_col = self._find_id_column(result)
            if id_col:
                result[id_col] = result[id_col].astype(str).str.strip()
                mask = result[id_col].isin(self._employee_ids)
                filtered_out = len(result) - mask.sum()
                result = result[mask].copy()
                result.attrs = df.attrs.copy()
                diagnostics['filtered_out_by_ids'] = int(filtered_out)
                logger.info(f"Filtered by employee IDs: {len(result)} employees (removed {filtered_out})")
            else:
                logger.warning("No ID column found for employee_ids filter")

        diagnostics['matched_rows'] = len(result)
        return result, diagnostics

    def _find_id_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "id" in col_lower and "empleado" in col_lower:
                return col
            if col_lower == "id empleado":
                return col
        return None

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._extract_sales_compliance(result)
        result = self._extract_productivity(result)
        result = self._calculate_sales_compliance_payment(result)
        result = self._calculate_productivity_payment(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _extract_sales_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance_col = self._find_column(result, "% cumplimiento", "cumplimiento")
        if compliance_col:
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["cumplimiento venta"] = compliance
        else:
            result["cumplimiento venta"] = 0

        actual_col = self._find_column(result, "actual")
        if actual_col:
            result["resultado venta"] = pd.to_numeric(result[actual_col], errors="coerce").fillna(0)
        else:
            result["resultado venta"] = 0

        budget_col = self._find_column(result, "budget")
        if budget_col:
            result["meta venta"] = pd.to_numeric(result[budget_col], errors="coerce").fillna(0)
        else:
            result["meta venta"] = 0

        return result

    def _extract_productivity(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        productivity_col = self._find_column(result, "productividad")
        if productivity_col:
            productivity = pd.to_numeric(result[productivity_col], errors="coerce").fillna(0)
            if productivity.max() > 2:
                productivity = productivity / 100
            result["cumplimiento productividad"] = productivity
            return result

        secondary_arrays = df.attrs.get('secondary_arrays', {})
        for key, sec_df in secondary_arrays.items():
            if "productividad" in key.lower():
                result = self._merge_productivity_by_rut(result, sec_df)
                return result

        result["cumplimiento productividad"] = 0
        return result

    def _merge_productivity_by_rut(
        self, df: pd.DataFrame, productivity_df: pd.DataFrame
    ) -> pd.DataFrame:
        result = df.copy()

        rut_col = self._find_column(productivity_df, "rut")
        prod_col = self._find_column(productivity_df, "productividad")

        if not rut_col or not prod_col:
            logger.warning("Productividad array missing RUT or Productividad column")
            result["cumplimiento productividad"] = 0
            return result

        prod_df = productivity_df[[rut_col, prod_col]].copy()
        prod_df.columns = ["_prod_rut", "_prod_value"]

        prod_df["_prod_rut"] = self._normalize_rut(prod_df["_prod_rut"])
        result["_emp_rut"] = self._normalize_rut(result["rut"])

        result = result.merge(prod_df, left_on="_emp_rut", right_on="_prod_rut", how="left")
        result = result.drop(columns=["_emp_rut", "_prod_rut"], errors="ignore")

        productivity = pd.to_numeric(result["_prod_value"], errors="coerce").fillna(0)
        if productivity.max() > 2:
            productivity = productivity / 100
        result["cumplimiento productividad"] = productivity
        result = result.drop(columns=["_prod_value"], errors="ignore")

        logger.info(f"Merged Productividad by RUT: {result['cumplimiento productividad'].notna().sum()} matches")
        return result

    def _normalize_rut(self, series: pd.Series) -> pd.Series:
        def clean_rut(rut: str) -> str:
            original = str(rut).strip()
            if original.upper() == "N/A" or original.lower() == "nan":
                return ""
            cleaned = original.replace(".", "").replace("-", "")
            if len(cleaned) >= 8:
                return cleaned[:-1]
            return cleaned
        return series.apply(clean_rut)

    def _calculate_sales_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["sales_compliance_payment"] = result["cumplimiento venta"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.SALES_COMPLIANCE_THRESHOLDS)
        )
        return result

    def _calculate_productivity_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["productivity_payment"] = result["cumplimiento productividad"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.PRODUCTIVITY_THRESHOLDS)
        )
        return result

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

    def _find_column(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None


TecnicoSiniestrosStrategy = ClaimsTechnicianStrategy
