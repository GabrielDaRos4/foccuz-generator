import logging

import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ServiceAdvisorStrategy(BaseScaniaStrategy):

    def __init__(self, excluded_ids: list[int] = None, **kwargs):
        super().__init__(**kwargs)
        self._excluded_ids = [str(eid) for eid in (excluded_ids or [])]

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        result, diagnostics = super()._filter_by_role_with_diagnostics(df)

        if self._excluded_ids:
            id_col = next(
                (c for c in result.columns if 'id' in c.lower() and 'empleado' in c.lower()),
                None
            )
            if id_col:
                before_count = len(result)
                result_ids = result[id_col].astype(str).str.strip()
                mask = ~result_ids.isin(self._excluded_ids)
                result = result[mask].copy()
                result.attrs = df.attrs.copy()
                diagnostics['excluded_ids'] = self._excluded_ids
                diagnostics['excluded_count'] = before_count - len(result)

        return result, diagnostics

    COLUMN_RENAME_MAP = {
        "venta": "Venta",
        "meta": "Meta",
        "cumplimiento venta": "Cumplimiento Venta",
        "pago_cumplimiento_venta": "Pago Cumplimiento Venta",
        "resultado nps": "Resultado NPS",
        "pago_nps": "Pago NPS",
        "cumplimiento productividad": "Cumplimiento Productividad",
        "pago_productividad": "Pago Productividad",
        "resultado wip": "Resultado WIP",
        "factor_wip": "Factor WIP",
        "resultado leadtime": "Resultado LeadTime",
        "factor_leadtime": "Factor LeadTime",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Resultado NPS", "Pago NPS",
        "Cumplimiento Productividad", "Pago Productividad",
        "Resultado WIP", "Factor WIP",
        "Resultado LeadTime", "Factor LeadTime",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Resultado NPS": "percentage",
        "Pago NPS": "money",
        "Cumplimiento Productividad": "percentage",
        "Pago Productividad": "money",
        "Resultado WIP": "percentage",
        "Factor WIP": "decimal",
        "Resultado LeadTime": "decimal",
        "Factor LeadTime": "decimal",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_compliance_data(result)
        result = self._extract_nps_data(result)
        result = self._extract_productivity_data(result)
        result = self._extract_wip_data(result)
        result = self._extract_leadtime_data(result)

        result = self._calculate_compliance_payment(result)
        result = self._calculate_nps_payment(result)
        result = self._calculate_productivity_payment(result)
        result = self._calculate_wip_factor(result)
        result = self._calculate_leadtime_factor(result)

        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)

        return result

    def _extract_compliance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance_col = self._find_column(result, "cumplimiento venta", "cumplimiento")
        if compliance_col and "productividad" not in compliance_col.lower():
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["cumplimiento venta"] = compliance
        else:
            result["cumplimiento venta"] = 0

        sales_col = self._find_column(result, "venta")
        if sales_col and "cumplimiento" not in sales_col.lower():
            result["venta"] = pd.to_numeric(result[sales_col], errors="coerce").fillna(0)
        elif "venta" not in result.columns:
            result["venta"] = 0

        budget_col = self._find_column(result, "meta", "budget")
        if budget_col:
            result["meta"] = pd.to_numeric(result[budget_col], errors="coerce").fillna(0)
        elif "meta" not in result.columns:
            result["meta"] = 0

        return result

    def _extract_nps_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        nps_col = self._find_column(result, "nps")
        if nps_col:
            nps = pd.to_numeric(result[nps_col], errors="coerce").fillna(0)
            if nps.max() > 2:
                nps = nps / 100
            result["resultado nps"] = nps
        else:
            result["resultado nps"] = 0

        return result

    def _extract_productivity_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        secondary_arrays = result.attrs.get('secondary_arrays', {})
        productivity_df = None
        for key, sec_df in secondary_arrays.items():
            if "productividad" in key.lower():
                productivity_df = sec_df.copy()
                break

        if productivity_df is not None and not productivity_df.empty:
            result = self._merge_productivity_by_rut(result, productivity_df)
        else:
            prod_col = self._find_column(result, "productividad", "cumplimiento productividad")
            if prod_col:
                prod = pd.to_numeric(result[prod_col], errors="coerce").fillna(0)
                if prod.max() > 2:
                    prod = prod / 100
                result["cumplimiento productividad"] = prod
            else:
                result["cumplimiento productividad"] = 0

        return result

    def _merge_productivity_by_rut(
        self, employees_df: pd.DataFrame, productivity_df: pd.DataFrame
    ) -> pd.DataFrame:
        productivity_df = productivity_df.copy()
        productivity_df.columns = productivity_df.columns.str.lower().str.strip()

        prod_col = next(
            (c for c in productivity_df.columns if "prod" in c.lower()),
            None
        )

        if not prod_col:
            employees_df["cumplimiento productividad"] = 0
            return employees_df

        rut_col = next(
            (c for c in productivity_df.columns if c == "rut"),
            None
        )

        if rut_col:
            productivity_df["_rut_clean"] = self._normalize_rut(productivity_df[rut_col])
            employees_df = employees_df.copy()
            employees_df["_rut_clean"] = self._normalize_rut(employees_df["rut"])

            prod_values = pd.to_numeric(productivity_df[prod_col], errors="coerce").fillna(0)
            if prod_values.max() > 2:
                prod_values = prod_values / 100
            productivity_df["cumplimiento productividad"] = prod_values

            employees_df = employees_df.merge(
                productivity_df[["_rut_clean", "cumplimiento productividad"]],
                on="_rut_clean",
                how="left"
            )
            employees_df = employees_df.drop(columns=["_rut_clean"], errors="ignore")
            employees_df["cumplimiento productividad"] = employees_df["cumplimiento productividad"].fillna(0)
        else:
            employees_df["cumplimiento productividad"] = 0

        return employees_df

    def _normalize_rut(self, series: pd.Series) -> pd.Series:
        def clean_rut(rut: str) -> str:
            original = str(rut).strip()
            if original.upper() == "N/A" or original.lower() == "nan":
                return ""
            cleaned = original.replace(".", "").replace("-", "")
            cleaned = ''.join(filter(str.isdigit, cleaned))
            if len(cleaned) >= 8:
                return cleaned[:-1]
            return cleaned
        return series.apply(clean_rut)

    def _extract_wip_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        wip_col = self._find_column(result, "wip")
        if wip_col:
            wip = pd.to_numeric(result[wip_col], errors="coerce").fillna(0)
            if wip.max() > 2:
                wip = wip / 100
            result["resultado wip"] = wip
        else:
            result["resultado wip"] = 0

        return result

    def _extract_leadtime_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        secondary_arrays = result.attrs.get('secondary_arrays', {})
        leadtime_df = None
        for key, sec_df in secondary_arrays.items():
            if "leadtime" in key.lower():
                leadtime_df = sec_df.copy()
                break

        if leadtime_df is not None and not leadtime_df.empty:
            result = self._merge_leadtime_by_rut(result, leadtime_df)
        else:
            lt_col = self._find_column(result, "leadtime", "lead_time")
            if lt_col:
                result["resultado leadtime"] = pd.to_numeric(
                    result[lt_col], errors="coerce"
                ).fillna(0)
            else:
                result["resultado leadtime"] = 0

        return result

    def _merge_leadtime_by_rut(
        self, employees_df: pd.DataFrame, leadtime_df: pd.DataFrame
    ) -> pd.DataFrame:
        leadtime_df = leadtime_df.copy()
        leadtime_df.columns = leadtime_df.columns.str.lower().str.strip()

        lt_col = next(
            (c for c in leadtime_df.columns if "leadtime" in c.lower()),
            None
        )

        if not lt_col:
            employees_df["resultado leadtime"] = 0
            return employees_df

        rut_col = next(
            (c for c in leadtime_df.columns if c == "rut"),
            None
        )

        if rut_col:
            leadtime_df["_rut_clean"] = self._normalize_rut(leadtime_df[rut_col])
            employees_df = employees_df.copy()
            employees_df["_rut_clean"] = self._normalize_rut(employees_df["rut"])

            leadtime_df["resultado leadtime"] = pd.to_numeric(
                leadtime_df[lt_col], errors="coerce"
            ).fillna(0)

            employees_df = employees_df.merge(
                leadtime_df[["_rut_clean", "resultado leadtime"]],
                on="_rut_clean",
                how="left"
            )
            employees_df = employees_df.drop(columns=["_rut_clean"], errors="ignore")
            employees_df["resultado leadtime"] = employees_df["resultado leadtime"].fillna(0)
        else:
            employees_df["resultado leadtime"] = 0

        return employees_df

    def _calculate_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        compliance = result["cumplimiento venta"]

        conditions = [
            compliance >= 1.20,
            compliance >= 1.15,
            compliance >= 1.10,
            compliance >= 1.05,
            compliance >= 1.00,
        ]
        choices = [750000, 650000, 550000, 500000, 450000]

        result["pago_cumplimiento_venta"] = np.select(conditions, choices, default=0)
        return result

    def _calculate_nps_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        nps = result["resultado nps"]

        conditions = [
            nps >= 1.00,
            nps >= 0.95,
            nps >= 0.90,
            nps >= 0.86,
            nps >= 0.80,
        ]
        choices = [150000, 110000, 80000, 60000, 30000]

        result["pago_nps"] = np.select(conditions, choices, default=0)
        return result

    def _calculate_productivity_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        productivity = result["cumplimiento productividad"]

        conditions = [
            productivity >= 1.00,
            productivity >= 0.95,
            productivity >= 0.90,
            productivity >= 0.85,
            productivity >= 0.83,
        ]
        choices = [150000, 120000, 90000, 60000, 42000]

        result["pago_productividad"] = np.select(conditions, choices, default=0)
        return result

    def _calculate_leadtime_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        leadtime = result["resultado leadtime"]

        conditions = [
            leadtime <= 3,
            (leadtime > 3) & (leadtime <= 4.99),
            (leadtime >= 5) & (leadtime <= 5.99),
            leadtime >= 6,
        ]
        choices = [1.05, 1.00, 0.95, 0.90]

        result["factor_leadtime"] = np.select(conditions, choices, default=0)
        return result

    def _calculate_wip_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        wip = result["resultado wip"]

        conditions = [
            wip >= 0.30,
            wip >= 0.26,
            wip >= 0.20,
            wip >= 0,
        ]
        choices = [0.90, 0.95, 1.00, 1.05]

        result["factor_wip"] = np.select(conditions, choices, default=0)
        return result

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        base_commission = (
            result["pago_cumplimiento_venta"].fillna(0) +
            result["pago_productividad"].fillna(0) +
            result["pago_nps"].fillna(0)
        )

        factor_avg = (
            result["factor_leadtime"].fillna(0) +
            result["factor_wip"].fillna(0)
        ) / 2

        result["commission"] = (base_commission * factor_avg).fillna(0)
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


AsesorServicioStrategy = ServiceAdvisorStrategy
