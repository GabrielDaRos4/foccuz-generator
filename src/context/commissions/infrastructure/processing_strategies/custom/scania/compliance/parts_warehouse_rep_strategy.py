import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class PartsWarehouseRepStrategy(BaseScaniaStrategy):

    COLUMN_RENAME_MAP = {
        "venta": "Venta",
        "meta": "Meta",
        "cumplimiento venta": "Cumplimiento Venta",
        "pago_cumplimiento_venta": "Pago Cumplimiento Venta",
        "uso dsm": "Uso DSM",
        "pago_dsm": "Pago DSM",
        "nivel dsm": "Nivel DSM",
        "pago_nivel_dsm": "Pago Nivel DSM",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Uso DSM", "Pago DSM", "Nivel DSM", "Pago Nivel DSM",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Uso DSM": "integer",
        "Pago DSM": "money",
        "Nivel DSM": "percentage",
        "Pago Nivel DSM": "money",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_compliance_data(result)
        result = self._extract_dsm_usage(result)
        result = self._extract_dsm_level(result)

        result = self._calculate_compliance_payment(result)
        result = self._calculate_dsm_payment(result)
        result = self._calculate_dsm_level_payment(result)

        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)

        return result

    def _extract_compliance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance_col = self._find_column(result, "cumplimiento")
        if compliance_col:
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["cumplimiento venta"] = compliance
        else:
            result["cumplimiento venta"] = 0

        sales_col = self._find_column(result, "actual", "venta")
        if sales_col and "cumplimiento" not in sales_col.lower():
            result["venta"] = pd.to_numeric(result[sales_col], errors="coerce").fillna(0)
        elif "venta" not in result.columns:
            result["venta"] = 0

        budget_col = self._find_column(result, "budget", "meta")
        if budget_col:
            result["meta"] = pd.to_numeric(result[budget_col], errors="coerce").fillna(0)
        elif "meta" not in result.columns:
            result["meta"] = 0

        return result

    def _extract_dsm_usage(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        uso_col = self._find_column(result, "usodsm", "uso dsm")
        if uso_col:
            result["uso dsm"] = pd.to_numeric(result[uso_col], errors="coerce").fillna(0)
        else:
            result["uso dsm"] = 0

        return result

    def _extract_dsm_level(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        nivel_col = self._find_column(result, "niveldsm", "nivel dsm")
        if nivel_col:
            nivel = pd.to_numeric(result[nivel_col], errors="coerce").fillna(0)
            if nivel.max() > 2:
                nivel = nivel / 100
            result["nivel dsm"] = nivel
        else:
            result["nivel dsm"] = 0

        return result

    def _calculate_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        def calc_payment(compliance: float) -> int:
            if pd.isna(compliance) or compliance < 1.00:
                return 0
            if compliance >= 1.22:
                return 370000
            base = 150000
            increment = int((compliance - 1.00) * 100)
            return base + increment * 10000

        result["pago_cumplimiento_venta"] = result["cumplimiento venta"].apply(calc_payment)
        return result

    def _calculate_dsm_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["pago_dsm"] = result["uso dsm"].apply(lambda x: 50000 if x == 1 else 0)
        return result

    def _calculate_dsm_level_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        def calc_payment(nivel: float) -> int:
            if pd.isna(nivel) or nivel < 0.80:
                return 0
            if nivel >= 0.90:
                return 180000
            base = 50000
            increment = int((nivel - 0.80) * 100)
            return base + increment * 13000

        result["pago_nivel_dsm"] = result["nivel dsm"].apply(calc_payment)
        return result

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["commission"] = (
            result["pago_cumplimiento_venta"].fillna(0) +
            result["pago_dsm"].fillna(0) +
            result["pago_nivel_dsm"].fillna(0)
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


VendedorRepuestosBodegaStrategy = PartsWarehouseRepStrategy
