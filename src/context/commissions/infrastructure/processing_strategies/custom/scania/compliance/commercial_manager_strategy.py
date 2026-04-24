import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class CommercialManagerStrategy(BaseScaniaStrategy):

    COMMISSION_TABLE = {
        50: {80: 700000, 90: 900000, 100: 1200000, 110: 1500000, 120: 2000000},
        60: {80: 900000, 90: 1000000, 100: 1500000, 110: 2000000, 120: 2500000},
        70: {80: 1000000, 90: 1200000, 100: 2000000, 110: 2500000, 120: 2800000},
        80: {80: 1200000, 90: 1500000, 100: 2500000, 110: 2800000, 120: 3200000},
    }

    EFFECTIVENESS_THRESHOLDS = [
        (95, float("inf"), 1.10),
        (90, 94.99, 1.05),
        (80, 89.99, 1.00),
        (0, 79.99, 0.90),
    ]

    DIO_THRESHOLDS = [
        (0, 30, 1.10),
        (31, 44, 1.05),
        (45, 55, 1.00),
        (56, float("inf"), 0.90),
    ]

    MARGIN_THRESHOLDS = [
        (15, float("inf"), 0.15),
        (12, 14.99, 0.10),
        (10, 11.99, 0.07),
        (8, 9.99, 0.05),
        (0, 7.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "unidades_nuevas_vendidas": "Unidades Nuevas Vendidas",
        "meta_ventas": "Meta Ventas",
        "cumplimiento_venta": "Cumplimiento De Venta",
        "penetracion_contratos": "Penetracion Contratos Zona",
        "margen_venta": "Margen De Venta",
        "factor_margen_venta": "Factor Margen Venta",
        "order_intake": "Order Intake",
        "cancelaciones": "Cancelaciones",
        "total_ordenes": "Total Ordenes",
        "efectividad_venta": "% Efectividad Venta",
        "cumplimiento_efectividad_venta": "Cumplimiento Efectividad Venta",
        "ponderacion_efectividad": "Ponderacion Efectividad",
        "dio": "DIO",
        "cumplimiento_dio": "Cumplimiento DIO",
        "ponderacion_dio": "Ponderacion DIO",
        "base_comision": "Base Comision",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Unidades Nuevas Vendidas", "Meta Ventas", "Cumplimiento De Venta",
        "Penetracion Contratos Zona", "Margen De Venta", "Factor Margen Venta",
        "Order Intake", "Cancelaciones", "Total Ordenes",
        "% Efectividad Venta", "Cumplimiento Efectividad Venta", "Ponderacion Efectividad",
        "DIO", "Cumplimiento DIO", "Ponderacion DIO",
        "Base Comision", "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Unidades Nuevas Vendidas": "number",
        "Meta Ventas": "number",
        "Cumplimiento De Venta": "percentage",
        "Penetracion Contratos Zona": "percentage",
        "Margen De Venta": "percentage",
        "Factor Margen Venta": "percentage",
        "Order Intake": "number",
        "Cancelaciones": "number",
        "Total Ordenes": "number",
        "% Efectividad Venta": "percentage",
        "Cumplimiento Efectividad Venta": "number",
        "Ponderacion Efectividad": "percentage",
        "DIO": "number",
        "Cumplimiento DIO": "number",
        "Ponderacion DIO": "percentage",
        "Base Comision": "money",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': ["cargo2 contains 'jefe comercial'"],
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
            .str.contains("jefe comercial", na=False)
        )

        filtered_df = df[mask].copy()

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        logger.info(f"Filtered by cargo2 'jefe comercial': {len(filtered_df)} employees")

        return filtered_df, diagnostics

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_sales_data(result)
        result = self._extract_effectiveness_data(result)
        result = self._extract_dio_data(result)
        result = self._extract_margin_data(result)
        result = self._extract_orders_data(result)
        result = self._calculate_factors(result)
        result = self._calculate_base_commission(result)
        result = self._calculate_total_commission(result)

        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        unidades_col = self._find_column(result, "unidades nuevas vendidas", "unidades_nuevas")
        meta_col = self._find_column(result, "meta ventas", "meta_ventas")
        cumpl_col = self._find_column(result, "cumplimiento de venta", "cumplimiento_venta")
        pen_col = self._find_column(result, "penetracion contratos zona", "penetracion_contratos")

        if unidades_col:
            result["unidades_nuevas_vendidas"] = pd.to_numeric(
                result[unidades_col], errors="coerce"
            ).fillna(0)
        else:
            result["unidades_nuevas_vendidas"] = 0

        if meta_col:
            result["meta_ventas"] = pd.to_numeric(
                result[meta_col], errors="coerce"
            ).fillna(0)
        else:
            result["meta_ventas"] = 0

        if cumpl_col:
            result["cumplimiento_venta"] = pd.to_numeric(
                result[cumpl_col], errors="coerce"
            ).fillna(0)
            if result["cumplimiento_venta"].max() > 2:
                result["cumplimiento_venta"] = result["cumplimiento_venta"] / 100
        else:
            result["cumplimiento_venta"] = 0

        if pen_col:
            result["penetracion_contratos"] = pd.to_numeric(
                result[pen_col], errors="coerce"
            ).fillna(0)
            if result["penetracion_contratos"].max() > 2:
                result["penetracion_contratos"] = result["penetracion_contratos"] / 100
        else:
            result["penetracion_contratos"] = 0

        return result

    def _extract_effectiveness_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        efec_col = self._find_column(result, "% efectividad venta", "efectividad_venta")
        pond_efec_col = self._find_column(result, "ponderacion efectividad", "ponderacion_efectividad")

        if efec_col:
            result["efectividad_venta"] = pd.to_numeric(
                result[efec_col], errors="coerce"
            ).fillna(0)
            if result["efectividad_venta"].max() > 2:
                result["efectividad_venta"] = result["efectividad_venta"] / 100
        else:
            result["efectividad_venta"] = 0

        if pond_efec_col:
            result["ponderacion_efectividad"] = pd.to_numeric(
                result[pond_efec_col], errors="coerce"
            ).fillna(0)
            if result["ponderacion_efectividad"].max() > 2:
                result["ponderacion_efectividad"] = result["ponderacion_efectividad"] / 100
        else:
            result["ponderacion_efectividad"] = 0.5

        return result

    def _extract_dio_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        dio_col = self._find_column(result, "dio")
        pond_dio_col = self._find_column(result, "ponderacion dio", "ponderacion_dio")

        if dio_col:
            result["dio"] = pd.to_numeric(
                result[dio_col], errors="coerce"
            ).fillna(0)
        else:
            result["dio"] = 0

        if pond_dio_col:
            result["ponderacion_dio"] = pd.to_numeric(
                result[pond_dio_col], errors="coerce"
            ).fillna(0)
            if result["ponderacion_dio"].max() > 2:
                result["ponderacion_dio"] = result["ponderacion_dio"] / 100
        else:
            result["ponderacion_dio"] = 0.5

        return result

    def _extract_margin_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        margin_col = self._find_column(result, "margen de venta", "margen_venta")

        if margin_col:
            result["margen_venta"] = pd.to_numeric(
                result[margin_col], errors="coerce"
            ).fillna(0)
            if result["margen_venta"].max() > 2:
                result["margen_venta"] = result["margen_venta"] / 100
        else:
            result["margen_venta"] = 0

        return result

    def _extract_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        intake_col = self._find_column(result, "order intake", "order_intake")
        cancel_col = self._find_column(result, "cancelaciones")
        total_col = self._find_column(result, "total ordenes", "total_ordenes")

        if intake_col:
            result["order_intake"] = pd.to_numeric(
                result[intake_col], errors="coerce"
            ).fillna(0)
        else:
            result["order_intake"] = 0

        if cancel_col:
            result["cancelaciones"] = pd.to_numeric(
                result[cancel_col], errors="coerce"
            ).fillna(0)
        else:
            result["cancelaciones"] = 0

        if total_col:
            result["total_ordenes"] = pd.to_numeric(
                result[total_col], errors="coerce"
            ).fillna(0)
        else:
            result["total_ordenes"] = 0

        return result

    def _calculate_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["cumplimiento_efectividad_venta"] = result["efectividad_venta"].apply(
            lambda x: self._get_effectiveness_factor(x * 100)
        )

        result["cumplimiento_dio"] = result["dio"].apply(
            self._get_dio_factor
        )

        result["factor_margen_venta"] = result["margen_venta"].apply(
            lambda x: self._get_margin_factor(x * 100)
        )

        return result

    def _get_effectiveness_factor(self, pct: float) -> float:
        if pd.isna(pct):
            return 0.9
        for lower, upper, factor in self.EFFECTIVENESS_THRESHOLDS:
            if lower <= pct <= upper:
                return factor
        return 0.9

    def _get_dio_factor(self, dio: float) -> float:
        if pd.isna(dio):
            return 1.0
        for lower, upper, factor in self.DIO_THRESHOLDS:
            if lower <= dio <= upper:
                return factor
        return 0.9

    def _get_margin_factor(self, pct: float) -> float:
        if pd.isna(pct):
            return 0
        for lower, upper, factor in self.MARGIN_THRESHOLDS:
            if lower <= pct <= upper:
                return factor
        return 0

    def _calculate_base_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["base_comision"] = result.apply(
            lambda row: self._lookup_commission(
                row["penetracion_contratos"],
                row["cumplimiento_venta"]
            ),
            axis=1
        )

        return result

    def _lookup_commission(self, penetracion: float, cumplimiento: float) -> int:
        pen_pct = penetracion * 100
        cumpl_pct = cumplimiento * 100

        pen_keys = sorted(self.COMMISSION_TABLE.keys())
        pen_key = 0
        for p in pen_keys:
            if pen_pct >= p:
                pen_key = p
            else:
                break

        if pen_key == 0:
            return 0

        cumpl_keys = sorted(self.COMMISSION_TABLE[pen_key].keys())
        cumpl_key = 0
        for c in cumpl_keys:
            if cumpl_pct >= c:
                cumpl_key = c
            else:
                break

        if cumpl_key == 0:
            return 0

        return self.COMMISSION_TABLE[pen_key].get(cumpl_key, 0)

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["commission"] = (
            (result["base_comision"] * (1 + result["factor_margen_venta"])) *
            (
                (result["cumplimiento_efectividad_venta"] * result["ponderacion_efectividad"]) +
                (result["cumplimiento_dio"] * result["ponderacion_dio"])
            )
        ).fillna(0)

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


JefeComercialStrategy = CommercialManagerStrategy
