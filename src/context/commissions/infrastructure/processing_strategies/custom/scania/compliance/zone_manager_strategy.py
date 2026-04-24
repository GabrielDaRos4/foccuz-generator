import logging

import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ZoneManagerStrategy(BaseScaniaStrategy):

    BASE_PAYMENT_TABLE = [
        [250000, 300000, 500000, 600000, 800000],
        [300000, 500000, 600000, 1100000, 1300000],
        [500000, 600000, 1100000, 1300000, 1500000],
        [600000, 1100000, 1300000, 1500000, 2000000],
        [800000, 1300000, 1500000, 2000000, 2500000],
    ]

    SERVICE_LEVELS = [0.8, 0.9, 1.0, 1.1, 1.2]
    SALES_LEVELS = [0.8, 0.9, 1.0, 1.1, 1.2]

    MARGIN_THRESHOLDS = [
        (0.00, 0.0799, 0.00),
        (0.08, 0.0999, 0.05),
        (0.10, 0.1199, 0.07),
        (0.12, 0.1499, 0.10),
        (0.15, float("inf"), 0.15),
    ]

    MINIMUM_COMPLIANCE = 0.8

    COLUMN_RENAME_MAP = {
        "cumplimiento meta servicio": "Cumplimiento Meta Servicio",
        "cumplimiento meta venta": "Cumplimiento Meta Venta",
        "margen venta": "Margen Venta",
        "base_payment": "Pago Base",
        "margin_factor": "Factor Margen",
        "base_payment_prorated": "Pago Base Proporcional",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Meta Servicio", "Cumplimiento Meta Venta",
        "Margen Venta", "Pago Base", "Factor Margen",
        "Pago Base Proporcional",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento Meta Servicio": "percentage",
        "Cumplimiento Meta Venta": "percentage",
        "Margen Venta": "percentage",
        "Pago Base": "money",
        "Factor Margen": "percentage",
        "Pago Base Proporcional": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._enrich_from_margenes_array(result)
        result = self._extract_compliance_values(result)
        result = self._extract_margin_value(result)
        result = self._calculate_base_payment(result)
        result = self._calculate_margin_factor(result)
        result = self._calculate_final_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _enrich_from_margenes_array(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        margenes_df = secondary_arrays.get('Margenes y ventas')
        if margenes_df is None or margenes_df.empty:
            logger.warning("No 'Margenes y ventas' secondary array found")
            return result

        margenes_df = margenes_df.copy()
        margenes_df.columns = margenes_df.columns.str.lower().str.strip()

        rut_col = next((c for c in margenes_df.columns if c == 'rut'), None)
        if not rut_col:
            logger.warning("No RUT column found in Margenes y ventas")
            return result

        emp_rut_col = next((c for c in result.columns if 'rut' in c.lower()), None)
        if not emp_rut_col:
            logger.warning("No RUT column found in employee data")
            return result

        from src.context.commissions.infrastructure.processing_strategies.custom.scania.merge.normalizers import (
            normalize_rut,
        )

        result["_rut_merge"] = normalize_rut(result[emp_rut_col])
        margenes_df["_rut_merge"] = normalize_rut(margenes_df[rut_col])

        new_cols = [c for c in margenes_df.columns if c not in result.columns and c != "_rut_merge"]
        merge_cols = ["_rut_merge"] + new_cols

        margenes_subset = margenes_df[merge_cols].drop_duplicates(subset=["_rut_merge"])
        result = result.merge(margenes_subset, on="_rut_merge", how="left")
        result = result.drop(columns=["_rut_merge"], errors="ignore")

        logger.info(f"Enriched with Margenes y ventas: added {len(new_cols)} columns")
        return result

    def _extract_compliance_values(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        service_col = self._find_service_compliance_column(result)
        if service_col:
            result["cumplimiento meta servicio"] = self._normalize_compliance(result[service_col])
        else:
            result["cumplimiento meta servicio"] = 0

        sales_col = self._find_sales_compliance_column(result)
        if sales_col:
            result["cumplimiento meta venta"] = self._normalize_compliance(result[sales_col])
        else:
            result["cumplimiento meta venta"] = 0

        return result

    def _find_service_compliance_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "servicio" in col_lower and "cumplimiento" in col_lower:
                return col
        for col in df.columns:
            col_lower = col.lower()
            if col_lower == "% cumplimiento" or col_lower == "cumplimiento":
                return col
        return None

    def _find_sales_compliance_column(self, df: pd.DataFrame) -> str | None:
        patterns = ["cumplimientoventa", "cumplimiento venta"]
        for col in df.columns:
            col_lower = col.lower().replace(" ", "")
            for pattern in patterns.copy():
                pattern_clean = pattern.replace(" ", "")
                if pattern_clean in col_lower and "servicio" not in col_lower:
                    return col
        for col in df.columns:
            col_lower = col.lower()
            if "venta" in col_lower and "cumplimiento" in col_lower and "servicio" not in col_lower:
                return col
        return None

    def _normalize_compliance(self, series: pd.Series) -> pd.Series:
        values = pd.to_numeric(series, errors="coerce").fillna(0)
        if values.max() > 2:
            values = values / 100
        return values

    def _extract_margin_value(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        margin_col = self._find_margin_column(result)
        if margin_col:
            margin_values = pd.to_numeric(result[margin_col], errors="coerce").fillna(0)
            if margin_values.max() > 2:
                margin_values = margin_values / 100
            result["margen venta"] = margin_values
        else:
            result["margen venta"] = 0

        return result

    def _find_margin_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "margen" in col.lower():
                return col
        return None

    def _calculate_base_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["base_payment"] = result.apply(self._get_base_payment_for_row, axis=1)
        return result

    def _get_base_payment_for_row(self, row) -> int:
        service = row.get("cumplimiento meta servicio", 0)
        sales = row.get("cumplimiento meta venta", 0)

        if pd.isna(service) or pd.isna(sales):
            return 0
        if service < self.MINIMUM_COMPLIANCE or sales < self.MINIMUM_COMPLIANCE:
            return 0

        return self._get_table_value(service, sales)

    def _get_table_value(self, service: float, sales: float) -> int:
        row_idx = self._get_level_index(service, self.SERVICE_LEVELS)
        col_idx = self._get_level_index(sales, self.SALES_LEVELS)
        return self.BASE_PAYMENT_TABLE[row_idx][col_idx]

    def _get_level_index(self, value: float, levels: list) -> int:
        count = sum(1 for level in levels if value >= level)
        return max(0, min(count - 1, len(levels) - 1))

    def _calculate_margin_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["margin_factor"] = result["margen venta"].apply(self._get_margin_factor)
        return result

    def _get_margin_factor(self, margin: float) -> float:
        if pd.isna(margin):
            return 0.0
        for lower, upper, factor in self.MARGIN_THRESHOLDS:
            if lower <= margin <= upper:
                return factor
        return 0.0

    def _calculate_final_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        service = result["cumplimiento meta servicio"]
        sales = result["cumplimiento meta venta"]
        base = result["base_payment"]
        margin_factor = result["margin_factor"]

        both_at_100 = (service >= 1.0) & (sales >= 1.0)
        result["commission"] = np.where(
            both_at_100,
            base * (1 + margin_factor),
            base
        )

        result = self._apply_days_proration(result, "commission")

        result["base_payment_prorated"] = (
            result["base_payment"] * result["days_worked"] / self.DAYS_PER_MONTH
        )

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


JefeZonaStrategy = ZoneManagerStrategy
