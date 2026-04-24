import logging

import pandas as pd

from .generic_compliance_strategy import GenericComplianceStrategy

logger = logging.getLogger(__name__)


class PartsSalesRepStrategy(GenericComplianceStrategy):

    COMPLIANCE_THRESHOLDS = [
        (121, float("inf"), 660000),
        (120, 120.99, 650000),
        (119, 119.99, 640000),
        (118, 118.99, 630000),
        (117, 117.99, 620000),
        (116, 116.99, 610000),
        (115, 115.99, 600000),
        (114, 114.99, 590000),
        (113, 113.99, 580000),
        (112, 112.99, 570000),
        (111, 111.99, 560000),
        (110, 110.99, 550000),
        (109, 109.99, 540000),
        (108, 108.99, 530000),
        (107, 107.99, 520000),
        (106, 106.99, 510000),
        (105, 105.99, 500000),
        (104, 104.99, 490000),
        (103, 103.99, 480000),
        (102, 102.99, 470000),
        (101, 101.99, 460000),
        (100, 100.99, 450000),
        (0, 99.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "meta": "Meta Venta",
        "venta": "Resultado Venta",
        "cumplimiento venta": "Cumplimiento Venta",
        "pago_cumplimiento": "Pago Cumplimiento Venta",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Meta Venta", "Resultado Venta", "Cumplimiento Venta",
        "Pago Cumplimiento Venta", "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Meta Venta": "money",
        "Resultado Venta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._extract_sales_data(result)
        result = self._calculate_compliance_payment(result)
        result = self._apply_days_proration(result, "pago_cumplimiento")
        result = self._apply_guaranteed_minimum(result)
        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        target_col = self._find_column_by_pattern(result, "budget", "meta")
        if target_col:
            result["meta"] = pd.to_numeric(result[target_col], errors="coerce").fillna(0)
        else:
            result["meta"] = 0

        sales_col = self._find_sales_column(result)
        if sales_col:
            result["venta"] = pd.to_numeric(result[sales_col], errors="coerce").fillna(0)
        else:
            result["venta"] = 0

        compliance_col = self._find_compliance_column(result)
        if compliance_col:
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["cumplimiento venta"] = compliance
        else:
            result["cumplimiento venta"] = 0

        return result

    def _find_sales_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower().replace("-", "").replace(" ", "")
            if "cumplimiento" in col_lower or "budget" in col_lower or "meta" in col_lower:
                continue
            if "actual" in col_lower or "venta" in col_lower:
                return col
        return None

    def _find_compliance_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "%" in col_lower or "cumplimiento" in col_lower:
                return col
        return None

    def _find_column_by_pattern(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for col in df.columns:
            col_lower = col.lower().replace(" ", "").replace("-", "")
            for pattern in patterns:
                if pattern in col_lower:
                    return col
        return None

    def _calculate_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["pago_cumplimiento"] = result["cumplimiento venta"].apply(
            lambda x: self._get_threshold_payment(x * 100)
        )
        return result

    def _get_threshold_payment(self, pct: float) -> int:
        if pd.isna(pct):
            return 0
        for lower, upper, payment in self.COMPLIANCE_THRESHOLDS:
            if lower <= pct <= upper:
                return payment
        return 0

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


VendedorRepuestosStrategy = PartsSalesRepStrategy
