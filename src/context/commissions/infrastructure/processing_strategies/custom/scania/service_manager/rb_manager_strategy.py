import logging

import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class RBManagerStrategy(BaseScaniaStrategy):

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
                result_ids = result[id_col].astype(str).str.strip()
                mask = ~result_ids.isin(self._excluded_ids)
                excluded_count = (~mask).sum()
                result = result[mask].copy()
                diagnostics['excluded_ids'] = self._excluded_ids
                diagnostics['excluded_count'] = excluded_count
                logger.info(f"After excluding IDs {self._excluded_ids}: {len(result)} employees")

        return result, diagnostics

    SALES_THRESHOLDS = [
        (1.22, float("inf"), 520000),
        (1.21, 1.2199, 510000),
        (1.20, 1.2099, 500000),
        (1.19, 1.1999, 490000),
        (1.18, 1.1899, 480000),
        (1.17, 1.1799, 470000),
        (1.16, 1.1699, 460000),
        (1.15, 1.1599, 450000),
        (1.14, 1.1499, 440000),
        (1.13, 1.1399, 430000),
        (1.12, 1.1299, 420000),
        (1.11, 1.1199, 410000),
        (1.10, 1.1099, 400000),
        (1.09, 1.0999, 390000),
        (1.08, 1.0899, 380000),
        (1.07, 1.0799, 370000),
        (1.06, 1.0699, 360000),
        (1.05, 1.0599, 350000),
        (1.04, 1.0499, 340000),
        (1.03, 1.0399, 330000),
        (1.02, 1.0299, 320000),
        (1.01, 1.0199, 310000),
        (1.00, 1.0099, 300000),
        (0, 0.9999, 0),
    ]

    DSM_USAGE_PAYMENT = 100000

    DSM_LEVEL_THRESHOLDS = [
        (0.90, float("inf"), 250000),
        (0.89, 0.8999, 235000),
        (0.88, 0.8899, 220000),
        (0.87, 0.8799, 205000),
        (0.86, 0.8699, 190000),
        (0.85, 0.8599, 175000),
        (0.84, 0.8499, 160000),
        (0.83, 0.8399, 145000),
        (0.82, 0.8299, 130000),
        (0.81, 0.8199, 115000),
        (0.80, 0.8099, 100000),
        (0, 0.7999, 0),
    ]

    COLUMN_RENAME_MAP = {
        "venta": "Venta",
        "meta": "Meta",
        "cumplimiento venta": "Cumplimiento Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "uso dsm": "Uso DSM",
        "dsm_usage_payment": "Pago DSM",
        "nivel dsm": "Nivel DSM",
        "dsm_level_payment": "Pago Nivel DSM",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Site_ID", "Sucursal",
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Uso DSM", "Pago DSM", "Nivel DSM", "Pago Nivel DSM",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Pago DSM": "money",
        "Nivel DSM": "percentage",
        "Pago Nivel DSM": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._extract_sales_data(result)
        result = self._calculate_sales_compliance_payment(result)
        result = self._calculate_dsm_usage_payment(result)
        result = self._calculate_dsm_level_payment(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        sales_col = self._find_sales_column(result)
        if sales_col:
            result["venta"] = pd.to_numeric(result[sales_col], errors="coerce").fillna(0)
        else:
            result["venta"] = 0

        target_col = self._find_target_column(result)
        if target_col:
            result["meta"] = pd.to_numeric(result[target_col], errors="coerce").fillna(0)
        else:
            result["meta"] = 0

        compliance_col = self._find_compliance_column(result)
        if compliance_col:
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["cumplimiento venta"] = compliance
        else:
            result["cumplimiento venta"] = np.where(
                result["meta"] > 0,
                result["venta"] / result["meta"],
                0
            )

        return result

    def _find_sales_column(self, df: pd.DataFrame) -> str | None:
        patterns = ["venta", "actual", "ventas"]
        for col in df.columns:
            col_lower = col.lower().replace("-", "").replace(" ", "")
            if "cumplimiento" in col_lower:
                continue
            for pattern in patterns:
                if pattern in col_lower:
                    return col
        return None

    def _find_target_column(self, df: pd.DataFrame) -> str | None:
        patterns = ["meta", "budget", "budget1"]
        for col in df.columns:
            col_lower = col.lower().replace("-", "").replace(" ", "")
            for pattern in patterns:
                if pattern in col_lower:
                    return col
        return None

    def _find_compliance_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "cumplimiento" in col_lower:
                return col
            if "% cumplimiento" in col_lower:
                return col
        return None

    def _calculate_sales_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["sales_compliance_payment"] = result["cumplimiento venta"].apply(
            self._get_sales_payment
        )
        return result

    def _get_sales_payment(self, compliance: float) -> int:
        if pd.isna(compliance):
            return 0
        for lower, upper, payment in self.SALES_THRESHOLDS:
            if lower <= compliance <= upper:
                return payment
        return 0

    def _calculate_dsm_usage_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        dsm_col = self._find_dsm_usage_column(result)

        if dsm_col:
            result["uso dsm"] = result[dsm_col]
            dsm_values = pd.to_numeric(result[dsm_col], errors="coerce").fillna(0)
            result["dsm_usage_payment"] = np.where(
                dsm_values == 1,
                self.DSM_USAGE_PAYMENT,
                0
            )
        else:
            result["uso dsm"] = 0
            result["dsm_usage_payment"] = 0

        return result

    def _find_dsm_usage_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "uso" in col_lower and "dsm" in col_lower:
                return col
        return None

    def _calculate_dsm_level_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        level_col = self._find_dsm_level_column(result)

        if level_col:
            level_values = pd.to_numeric(result[level_col], errors="coerce").fillna(0)
            if level_values.max() > 2:
                level_values = level_values / 100
            result["nivel dsm"] = level_values
            result["dsm_level_payment"] = level_values.apply(self._get_dsm_level_payment)
        else:
            result["nivel dsm"] = 0
            result["dsm_level_payment"] = 0

        return result

    def _find_dsm_level_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "nivel" in col_lower and "dsm" in col_lower:
                return col
        return None

    def _get_dsm_level_payment(self, level: float) -> int:
        if pd.isna(level):
            return 0
        for lower, upper, payment in self.DSM_LEVEL_THRESHOLDS:
            if lower <= level <= upper:
                return payment
        return 0

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["commission"] = (
            result["sales_compliance_payment"].fillna(0) +
            result["dsm_usage_payment"].fillna(0) +
            result["dsm_level_payment"].fillna(0)
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


EncargadoRBStrategy = RBManagerStrategy
