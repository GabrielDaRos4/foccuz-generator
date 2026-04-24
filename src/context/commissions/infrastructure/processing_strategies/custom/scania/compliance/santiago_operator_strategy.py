import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class SantiagoOperatorStrategy(BaseScaniaStrategy):

    COUNTRY_SALES_THRESHOLDS = [
        (110, float("inf"), 100000),
        (100, 109.99, 85000),
        (90, 99.99, 65000),
        (0, 89.99, 0),
    ]

    SERVICE_LEVEL_THRESHOLDS = [
        (98, float("inf"), 50000),
        (96, 97.99, 25000),
        (0, 95.99, 0),
    ]

    INVENTORY_ADJUSTMENT_THRESHOLDS = [
        (0, 1.49, 25000),
        (1.5, 3.0, 15000),
        (3.01, float("inf"), 0),
    ]

    COLUMN_RENAME_MAP = {
        "venta": "Venta",
        "meta": "Meta",
        "cumplimiento_venta": "Cumplimiento Venta",
        "pago_cumplimiento_venta": "Pago Cumplimiento Venta Pais",
        "nivel_servicio": "Nivel De Servicio",
        "pago_nivel_servicio": "Pago Nivel De Servicio",
        "ajuste_inventario": "Ajuste Inventario",
        "pago_ajuste_inventario": "Pago Ajuste Inventario",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta Pais",
        "Nivel De Servicio", "Pago Nivel De Servicio",
        "Ajuste Inventario", "Pago Ajuste Inventario",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta Pais": "money",
        "Nivel De Servicio": "percentage",
        "Pago Nivel De Servicio": "money",
        "Ajuste Inventario": "percentage",
        "Pago Ajuste Inventario": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': ["cargo2 contains 'ope. bodega santiago'"],
            'filtered_out_by_role': 0,
        }

        cargo2_col = next(
            (c for c in df.columns if "cargo2" in c.lower()), None
        )

        if not cargo2_col:
            logger.warning("No cargo2 column found, returning empty")
            diagnostics['no_cargo2_column'] = True
            return pd.DataFrame(), diagnostics

        mask = (
            df[cargo2_col]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.contains("ope. bodega santiago", na=False)
        )

        filtered_df = df[mask].copy()

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        logger.info(f"Filtered by cargo2 'ope. bodega santiago': {len(filtered_df)} employees")

        return filtered_df, diagnostics

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_sales_data(result)
        result = self._calculate_sales_payment(result)
        result = self._extract_service_level_data(result)
        result = self._calculate_service_level_payment(result)
        result = self._extract_inventory_adjustment_data(result)
        result = self._calculate_inventory_adjustment_payment(result)
        result = self._calculate_total_commission(result)

        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        venta_col = self._find_column(result, "ventarepuestospais", "venta")
        meta_col = self._find_column(result, "metarepuestospais", "meta")
        cumpl_col = self._find_column(result, "factcumpl", "cumplimiento")

        if venta_col:
            result["venta"] = pd.to_numeric(result[venta_col], errors="coerce").fillna(0)
        else:
            result["venta"] = 0

        if meta_col:
            result["meta"] = pd.to_numeric(result[meta_col], errors="coerce").fillna(0)
        else:
            result["meta"] = 0

        if cumpl_col:
            result["cumplimiento_venta"] = pd.to_numeric(
                result[cumpl_col], errors="coerce"
            ).fillna(0)
            if result["cumplimiento_venta"].max() > 2:
                result["cumplimiento_venta"] = result["cumplimiento_venta"] / 100
        else:
            result["cumplimiento_venta"] = result.apply(
                lambda row: row["venta"] / row["meta"] if row["meta"] > 0 else 0,
                axis=1
            )

        return result

    def _calculate_sales_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_cumplimiento_venta"] = result["cumplimiento_venta"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.COUNTRY_SALES_THRESHOLDS)
        )

        return result

    def _extract_service_level_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        service_col = self._find_column(result, "factnslog", "nivel_servicio", "ns_log")

        if service_col:
            result["nivel_servicio"] = pd.to_numeric(
                result[service_col], errors="coerce"
            ).fillna(0)
            if result["nivel_servicio"].max() > 2:
                result["nivel_servicio"] = result["nivel_servicio"] / 100
        else:
            result["nivel_servicio"] = 0

        return result

    def _calculate_service_level_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_nivel_servicio"] = result["nivel_servicio"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.SERVICE_LEVEL_THRESHOLDS)
        )

        return result

    def _extract_inventory_adjustment_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        inv_col = self._find_column(result, "difinvreal", "ajuste_inventario", "dif_inv")

        if inv_col:
            result["ajuste_inventario"] = pd.to_numeric(
                result[inv_col], errors="coerce"
            ).fillna(0)
            if result["ajuste_inventario"].max() > 10:
                result["ajuste_inventario"] = result["ajuste_inventario"] / 100
        else:
            result["ajuste_inventario"] = 0

        return result

    def _calculate_inventory_adjustment_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_ajuste_inventario"] = result["ajuste_inventario"].apply(
            lambda x: self._get_inventory_payment(x * 100 if x <= 1 else x)
        )

        return result

    def _get_inventory_payment(self, pct: float) -> int:
        if pd.isna(pct):
            return 0
        for lower, upper, payment in self.INVENTORY_ADJUSTMENT_THRESHOLDS:
            if lower <= pct <= upper:
                return payment
        return 0

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
            result["pago_cumplimiento_venta"].fillna(0) +
            result["pago_nivel_servicio"].fillna(0) +
            result["pago_ajuste_inventario"].fillna(0)
        )

        result = self._apply_days_proration(result, "commission")
        result = self._apply_guaranteed_minimum(result)

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


OperarioSantiagoStrategy = SantiagoOperatorStrategy
