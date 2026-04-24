import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ControlTowerStrategy(BaseScaniaStrategy):

    COLUMN_RENAME_MAP = {
        "cumplimiento venta": "Cumplimiento Venta",
        "resultado venta": "Resultado Venta",
        "meta venta": "Meta Venta",
        "pago_cumplimiento_venta": "Pago Cumplimiento Venta",
        "cumplimiento productividad": "Cumplimiento Productividad",
        "pago_productividad": "Pago Productividad",
        "cumplimiento eficiencia": "Cumplimiento Eficiencia",
        "pago_eficiencia": "Pago Eficiencia",
        "rechazo administrativo": "Rechazo Administrativo",
        "factor_rechazo": "Cumplimiento Rechazo Administrativo",
        "ebit": "EBIT",
        "factor_ebit": "Factor EBIT",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Meta Venta", "Resultado Venta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Cumplimiento Productividad", "Pago Productividad",
        "Cumplimiento Eficiencia", "Pago Eficiencia",
        "Rechazo Administrativo", "Cumplimiento Rechazo Administrativo",
        "EBIT", "Factor EBIT",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Meta Venta": "money",
        "Resultado Venta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Cumplimiento Productividad": "percentage",
        "Pago Productividad": "money",
        "Cumplimiento Eficiencia": "percentage",
        "Pago Eficiencia": "money",
        "Rechazo Administrativo": "integer",
        "Cumplimiento Rechazo Administrativo": "decimal",
        "EBIT": "percentage",
        "Factor EBIT": "decimal",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_cumplimiento_venta(result)
        result = self._extract_productividad(result)
        result = self._extract_eficiencia(result)
        result = self._extract_rechazo_admin(result)
        result = self._extract_ebit(result)

        result = self._calculate_pago_cumplimiento_venta(result)
        result = self._calculate_pago_productividad(result)
        result = self._calculate_pago_eficiencia(result)
        result = self._calculate_factor_rechazo(result)
        result = self._calculate_factor_ebit(result)

        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)

        return result

    def _extract_cumplimiento_venta(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def _extract_productividad(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        prod_col = self._find_column(result, "productividad")
        if prod_col:
            productivity = pd.to_numeric(result[prod_col], errors="coerce").fillna(0)
            if productivity.max() > 2:
                productivity = productivity / 100
            result["cumplimiento productividad"] = productivity
        else:
            result["cumplimiento productividad"] = 0

        return result

    def _extract_eficiencia(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        eff_col = self._find_column(result, "eficiencia")
        if eff_col:
            efficiency = pd.to_numeric(result[eff_col], errors="coerce").fillna(0)
            if efficiency.max() > 2:
                efficiency = efficiency / 100
            result["cumplimiento eficiencia"] = efficiency
        else:
            result["cumplimiento eficiencia"] = 0

        return result

    def _extract_rechazo_admin(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        rechazo_col = self._find_column(result, "rechazo", "qrechazo")
        if rechazo_col:
            result["rechazo administrativo"] = pd.to_numeric(
                result[rechazo_col], errors="coerce"
            ).fillna(0).astype(int)
        else:
            result["rechazo administrativo"] = 0

        return result

    def _extract_ebit(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        ebit_col = self._find_column(result, "ebit")
        if ebit_col:
            ebit = pd.to_numeric(result[ebit_col], errors="coerce").fillna(0)
            if ebit.max() > 2:
                ebit = ebit / 100
            result["ebit"] = ebit
        else:
            result["ebit"] = 0

        return result

    def _calculate_pago_cumplimiento_venta(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        def calc_payment(compliance: float) -> int:
            if pd.isna(compliance):
                return 0
            pct = compliance * 100
            if pct < 100:
                return 0
            if pct >= 122:
                return 570000
            base = 350000
            increment = int(pct) - 100
            return base + increment * 10000

        result["pago_cumplimiento_venta"] = result["cumplimiento venta"].apply(calc_payment)
        return result

    def _calculate_pago_productividad(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        def calc_payment(productivity: float) -> int:
            if pd.isna(productivity):
                return 0
            pct = productivity * 100
            if pct < 83:
                return 0
            if pct >= 100:
                return 74000
            base = 40000
            increment = int(pct) - 83
            return base + increment * 2000

        result["pago_productividad"] = result["cumplimiento productividad"].apply(calc_payment)
        return result

    def _calculate_pago_eficiencia(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        def calc_payment(efficiency: float) -> int:
            if pd.isna(efficiency):
                return 0
            pct = efficiency * 100
            if pct < 90:
                return 0
            if pct >= 110:
                return 140000
            base = 40000
            increment = int(pct) - 90
            return base + increment * 5000

        result["pago_eficiencia"] = result["cumplimiento eficiencia"].apply(calc_payment)
        return result

    def _calculate_factor_rechazo(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        def calc_factor(rechazo: int) -> float:
            if pd.isna(rechazo):
                return 1.0
            rechazo = int(rechazo)
            if rechazo == 0:
                return 1.0
            elif rechazo == 1:
                return 0.75
            elif rechazo == 2:
                return 0.5
            else:
                return 0.0

        result["factor_rechazo"] = result["rechazo administrativo"].apply(calc_factor)
        return result

    def _calculate_factor_ebit(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        def calc_factor(ebit: float) -> float:
            if pd.isna(ebit):
                return 0.0
            pct = ebit * 100
            if pct < 6:
                return 0.0
            elif pct < 11:
                return 0.05
            elif pct < 16:
                return 0.07
            elif pct < 21:
                return 0.10
            else:
                return 0.15

        result["factor_ebit"] = result["ebit"].apply(calc_factor)
        return result

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        base_commission = (
            result["pago_cumplimiento_venta"].fillna(0) +
            result["pago_productividad"].fillna(0) +
            result["pago_eficiencia"].fillna(0)
        )

        result["commission"] = (
            base_commission *
            result["factor_rechazo"].fillna(1.0) *
            (1 + result["factor_ebit"].fillna(0))
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


TorreControlStrategy = ControlTowerStrategy
