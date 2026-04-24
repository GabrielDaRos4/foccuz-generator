import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ClaimsAdvisorStrategy(BaseScaniaStrategy):

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
        (110, float("inf"), 680000),
        (105, 109.99, 580000),
        (100, 104.99, 480000),
        (95, 99.99, 380000),
        (90, 94.99, 280000),
        (0, 89.99, 0),
    ]

    LEADTIME_THRESHOLDS = [
        (0, 5.99, 200000),
        (6, 9.99, 150000),
        (10, 15.99, 120000),
        (16, float("inf"), 0),
    ]

    WIP_FACTORS = [
        (0, 19.99, 1.05),
        (20, 25.99, 1.00),
        (26, 29.99, 0.95),
        (30, float("inf"), 0.90),
    ]

    COLUMN_RENAME_MAP = {
        "cumplimiento venta": "Cumplimiento Venta",
        "resultado venta": "Resultado Venta",
        "meta venta": "Meta Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "leadtime": "Cump. Dias Prom. Facturacion",
        "leadtime_payment": "Pago Dias Prom. Facturacion",
        "wip": "% WIP",
        "wip_factor": "Factor % WIP",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Venta", "Resultado Venta", "Meta Venta",
        "Pago Cumplimiento Venta",
        "Cump. Dias Prom. Facturacion", "Pago Dias Prom. Facturacion",
        "% WIP", "Factor % WIP",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento Venta": "percentage",
        "Resultado Venta": "money",
        "Meta Venta": "money",
        "Pago Cumplimiento Venta": "money",
        "Cump. Dias Prom. Facturacion": "decimal",
        "Pago Dias Prom. Facturacion": "money",
        "% WIP": "percentage",
        "Factor % WIP": "decimal",
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
        result = self._extract_leadtime(result)
        result = self._extract_wip(result)
        result = self._calculate_sales_compliance_payment(result)
        result = self._calculate_leadtime_payment(result)
        result = self._calculate_wip_factor(result)
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

    def _extract_leadtime(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        leadtime_col = self._find_column(result, "leadtime", "lead_time", "dias prom")
        if leadtime_col:
            result["leadtime"] = pd.to_numeric(result[leadtime_col], errors="coerce").fillna(0)
        else:
            secondary_arrays = df.attrs.get('secondary_arrays', {})
            for key, sec_df in secondary_arrays.items():
                if "leadtime" in key.lower():
                    result = self._merge_leadtime_by_rut(result, sec_df)
                    return result
            result["leadtime"] = 0

        return result

    def _merge_leadtime_by_rut(
        self, df: pd.DataFrame, leadtime_df: pd.DataFrame
    ) -> pd.DataFrame:
        result = df.copy()

        rut_col = self._find_column(leadtime_df, "rut")
        lead_col = self._find_column(leadtime_df, "leadtime", "lead")

        if not rut_col or not lead_col:
            logger.warning("LeadTime array missing RUT or LeadTime column")
            result["leadtime"] = 0
            return result

        lead_df = leadtime_df[[rut_col, lead_col]].copy()
        lead_df.columns = ["_lead_rut", "_lead_value"]

        lead_df["_lead_rut"] = self._normalize_rut(lead_df["_lead_rut"])
        result["_emp_rut"] = self._normalize_rut(result["rut"])

        result = result.merge(lead_df, left_on="_emp_rut", right_on="_lead_rut", how="left")
        result = result.drop(columns=["_emp_rut", "_lead_rut"], errors="ignore")

        result["leadtime"] = pd.to_numeric(result["_lead_value"], errors="coerce").fillna(0)
        result = result.drop(columns=["_lead_value"], errors="ignore")

        logger.info(f"Merged LeadTime by RUT: {result['leadtime'].notna().sum()} matches")
        return result

    def _extract_wip(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        wip_col = self._find_column(result, "wip")
        if wip_col:
            wip = pd.to_numeric(result[wip_col], errors="coerce").fillna(0)
            if wip.max() > 2:
                wip = wip / 100
            result["wip"] = wip
        else:
            result["wip"] = 0

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

    def _calculate_leadtime_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["leadtime_payment"] = result["leadtime"].apply(
            lambda x: self._get_threshold_payment(x, self.LEADTIME_THRESHOLDS)
        )
        return result

    def _calculate_wip_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["wip_factor"] = result["wip"].apply(
            lambda x: self._get_wip_factor(x * 100)
        )
        return result

    def _get_threshold_payment(self, value: float, thresholds: list) -> int:
        if pd.isna(value):
            return 0
        for lower, upper, payment in thresholds:
            if lower <= value <= upper:
                return payment
        return 0

    def _get_wip_factor(self, pct: float) -> float:
        if pd.isna(pct):
            return 1.0
        for lower, upper, factor in self.WIP_FACTORS:
            if lower <= pct <= upper:
                return factor
        return 1.0

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["commission"] = (
            (result["sales_compliance_payment"].fillna(0) +
             result["leadtime_payment"].fillna(0)) *
            result["wip_factor"].fillna(1.0)
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


AsesorSiniestrosStrategy = ClaimsAdvisorStrategy
