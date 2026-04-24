import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ServiceManagerStrategy(BaseScaniaStrategy):

    COLUMN_RENAME_MAP = {
        "sales_compliance": "Cumplimiento Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "nps_result": "Resultado NPS",
        "nps_compliance_payment": "Pago NPS",
        "team_management_compliance": "Cumplimiento Gestión Equipo",
        "wip_factor": "Factor WIP",
        "ebit_factor": "Factor EBIT",
        "final_payment": "Pago Final",
        "guaranteed": "Garantizado",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Resultado NPS", "Pago NPS",
        "Cumplimiento Gestión Equipo", "Factor WIP", "Factor EBIT",
        "Pago Final", "Garantizado",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Resultado NPS": "percentage",
        "Pago NPS": "money",
        "Cumplimiento Gestión Equipo": "percentage",
        "Factor WIP": "percentage",
        "Factor EBIT": "percentage",
        "Pago Final": "money",
        "Garantizado": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    SALES_THRESHOLDS_LARGE = [
        (1.20, float("inf"), 1000000),
        (1.15, 1.1999, 850000),
        (1.10, 1.1499, 750000),
        (1.05, 1.0999, 650000),
        (1.00, 1.0499, 600000),
        (0, 0.9999, 0),
    ]

    SALES_THRESHOLDS_MEDIUM = [
        (1.20, float("inf"), 950000),
        (1.15, 1.1999, 820000),
        (1.10, 1.1499, 720000),
        (1.05, 1.0999, 620000),
        (1.00, 1.0499, 570000),
        (0, 0.9999, 0),
    ]

    SALES_THRESHOLDS_SMALL = [
        (1.20, float("inf"), 900000),
        (1.15, 1.1999, 770000),
        (1.10, 1.1499, 680000),
        (1.05, 1.0999, 600000),
        (1.00, 1.0499, 550000),
        (0, 0.9999, 0),
    ]

    NPS_THRESHOLDS = [
        (1.00, float("inf"), 150000),
        (0.95, 0.9999, 120000),
        (0.90, 0.9499, 90000),
        (0.86, 0.8999, 70000),
        (0.80, 0.8599, 30000),
        (0, 0.7999, 0),
    ]

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._calculate_sales_payment(result)
        result = self._calculate_nps_payment(result)
        result = self._calculate_team_management(result)
        result = self._calculate_wip_factor(result)
        result = self._calculate_ebit_factor(result)

        result = self._ensure_numeric_columns(result)
        result = self._calculate_final_payment(result)
        result = self._apply_guaranteed_minimum(result)

        return result

    def _ensure_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        numeric_cols = [
            "sales_compliance_payment",
            "nps_compliance_payment",
            "team_management_compliance",
            "wip_factor",
            "ebit_factor"
        ]
        for col in numeric_cols:
            if col not in result.columns:
                default = 0 if "payment" in col or "compliance" in col else 1
                result[col] = default
            result[col] = pd.to_numeric(result[col], errors="coerce").fillna(
                0 if "payment" in col or "compliance" in col else 1
            )
        return result

    def _calculate_final_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["final_payment"] = (
            (result["sales_compliance_payment"] + result["nps_compliance_payment"])
            * result["team_management_compliance"]
            * result["wip_factor"]
            * (1 + result["ebit_factor"])
        ).fillna(0)

        result = self._apply_days_proration(result, "final_payment")
        return result

    def _apply_guaranteed_minimum(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        guaranteed_col = next(
            (c for c in result.columns if "garantizado" in c), None
        )
        if guaranteed_col:
            result["guaranteed"] = pd.to_numeric(
                result[guaranteed_col].replace("NA", 0), errors="coerce"
            ).fillna(0)
        else:
            result["guaranteed"] = 0

        result["commission"] = result[["commission", "guaranteed"]].max(axis=1)
        return result

    def _calculate_sales_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance = self._get_sales_compliance(result)
        result["sales_compliance"] = compliance

        size = self._get_branch_size(result)
        result["branch_size"] = size

        result["sales_compliance_payment"] = result.apply(
            self._get_sales_payment_for_row, axis=1
        )
        return result

    def _get_sales_compliance(self, df: pd.DataFrame) -> pd.Series:
        compliance_col = next(
            (c for c in df.columns if "cumplimiento" in c and "venta" in c), None
        )
        if compliance_col:
            compliance = pd.to_numeric(df[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            return compliance

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

    def _get_branch_size(self, df: pd.DataFrame) -> pd.Series:
        size_col = next(
            (c for c in df.columns if "tamano" in c or "tamaño" in c), None
        )
        if size_col:
            return df[size_col].astype(str).str.strip().str.lower()
        return pd.Series(["medium"] * len(df), index=df.index)

    def _get_sales_payment_for_row(self, row) -> int:
        compliance = row.get("sales_compliance", 0)
        size = row.get("branch_size", "medium")

        if pd.isna(compliance) or compliance < 1.0:
            return 0

        thresholds = self._get_thresholds_for_size(size)
        for lower, upper, payment in thresholds:
            if lower <= compliance <= upper:
                return payment
        return 0

    def _get_thresholds_for_size(self, size: str) -> list[tuple]:
        if "grande" in str(size) or "large" in str(size):
            return self.SALES_THRESHOLDS_LARGE
        if "pequeña" in str(size) or "pequena" in str(size) or "small" in str(size):
            return self.SALES_THRESHOLDS_SMALL
        return self.SALES_THRESHOLDS_MEDIUM

    def _calculate_nps_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        nps_col = next((c for c in result.columns if "nps" in c), None)
        if nps_col:
            result["nps_result"] = pd.to_numeric(
                result[nps_col], errors="coerce"
            ).fillna(0)
            if result["nps_result"].max() > 2:
                result["nps_result"] = result["nps_result"] / 100
        else:
            result["nps_result"] = 0

        result["nps_compliance_payment"] = result["nps_result"].apply(
            self._get_nps_payment
        )
        return result

    def _get_nps_payment(self, nps: float) -> int:
        if pd.isna(nps) or nps < 0.80:
            return 0
        for lower, upper, payment in self.NPS_THRESHOLDS:
            if lower <= nps <= upper:
                return payment
        return 0

    def _calculate_team_management(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["accident_compliance"] = self._get_accident_compliance(result)
        result["turnover_compliance"] = self._get_turnover_compliance(result)
        result["absenteeism_compliance"] = self._get_absenteeism_compliance(result)

        result["team_management_compliance"] = (
            result["accident_compliance"] +
            result["turnover_compliance"] +
            result["absenteeism_compliance"]
        ) / 3

        return result

    def _get_accident_compliance(self, df: pd.DataFrame) -> pd.Series:
        acc_col = next(
            (c for c in df.columns if "accidentabilidad" in c or
             ("cumplimiento" in c and "accid" in c)), None
        )
        if acc_col:
            values = pd.to_numeric(df[acc_col], errors="coerce").fillna(0)
            if values.max() > 2:
                values = values / 100
            return values
        return pd.Series([1.0] * len(df), index=df.index)

    def _get_turnover_compliance(self, df: pd.DataFrame) -> pd.Series:
        turnover_col = next((c for c in df.columns if "rotacion" in c), None)
        if turnover_col:
            values = pd.to_numeric(df[turnover_col], errors="coerce").fillna(0)
            if values.max() > 2:
                values = values / 100
            return values.apply(self._turnover_to_compliance)
        return pd.Series([1.0] * len(df), index=df.index)

    def _get_absenteeism_compliance(self, df: pd.DataFrame) -> pd.Series:
        absence_col = next((c for c in df.columns if "ausentismo" in c), None)
        if absence_col:
            values = pd.to_numeric(df[absence_col], errors="coerce").fillna(0)
            if values.max() > 2:
                values = values / 100
            return values.apply(self._absenteeism_to_compliance)
        return pd.Series([1.0] * len(df), index=df.index)

    @staticmethod
    def _turnover_to_compliance(turnover: float) -> float:
        if pd.isna(turnover):
            return 1.0
        turnover_pct = turnover * 100
        if turnover_pct <= 1.00:
            return 1.10
        if turnover_pct <= 1.50:
            return 1.00
        if turnover_pct <= 2.00:
            return 0.90
        return 0.80

    @staticmethod
    def _absenteeism_to_compliance(absenteeism: float) -> float:
        if pd.isna(absenteeism):
            return 1.0
        absence_pct = absenteeism * 100
        if absence_pct <= 3.50:
            return 1.10
        if absence_pct <= 5.00:
            return 1.00
        if absence_pct <= 6.00:
            return 0.90
        return 0.80

    def _calculate_wip_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        wip_col = next((c for c in result.columns if "wip" in c), None)
        if wip_col:
            wip_values = pd.to_numeric(result[wip_col], errors="coerce").fillna(0)
            if wip_values.max() > 2:
                wip_values = wip_values / 100
            result["wip_factor"] = wip_values.apply(self._wip_to_factor)
        else:
            result["wip_factor"] = 1.0

        return result

    @staticmethod
    def _wip_to_factor(wip: float) -> float:
        if pd.isna(wip):
            return 1.0
        wip_pct = wip * 100
        if wip_pct <= 19.99:
            return 1.05
        if wip_pct <= 25.99:
            return 1.00
        if wip_pct <= 29.99:
            return 0.95
        return 0.90

    def _calculate_ebit_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        ebit_col = next((c for c in result.columns if "ebit" in c), None)
        if ebit_col:
            ebit_values = pd.to_numeric(result[ebit_col], errors="coerce").fillna(0)
            if ebit_values.max() > 2:
                ebit_values = ebit_values / 100
            result["ebit_factor"] = ebit_values.apply(self._ebit_to_factor)
        else:
            result["ebit_factor"] = 0

        return result

    @staticmethod
    def _ebit_to_factor(ebit: float) -> float:
        if pd.isna(ebit):
            return 0
        ebit_pct = ebit * 100
        if ebit_pct <= 5.99:
            return 0.00
        if ebit_pct <= 10.99:
            return 0.10
        if ebit_pct <= 15.99:
            return 0.15
        if ebit_pct <= 20.99:
            return 0.20
        if ebit_pct <= 25.99:
            return 0.25
        return 0.30


JefeServicioStrategy = ServiceManagerStrategy
