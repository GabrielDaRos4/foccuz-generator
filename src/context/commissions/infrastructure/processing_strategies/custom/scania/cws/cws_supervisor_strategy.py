import logging

import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class CWSSupervisorStrategy(BaseScaniaStrategy):

    ADMIN_STOCK_THRESHOLDS = [
        (100, float("inf"), 200000),
        (96, 99.99, 100000),
        (91, 95.99, 50000),
        (0, 90.99, 0),
    ]

    FLEET_AVAILABILITY_THRESHOLDS = [
        (97, float("inf"), 100000),
        (90, 96.99, 50000),
        (0, 89.99, 0),
    ]

    SALES_COMPLIANCE_THRESHOLDS = [
        (100, float("inf"), 150000),
        (50, 99.99, 75000),
        (0, 49.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "diferencia_inventario": "Diferencia Inventario",
        "total_inventario": "Total Inventario",
        "pct_ajuste_inventario": "% Ajuste De Inventario",
        "cumplimiento_ajuste_inventario": "Cumplimiento Ajuste De Inventario",
        "inventario_rotativo_pendiente": "Inventario Rotativo Pendiente",
        "inventario_rotativo_total": "Inventario Rotativo Total",
        "cumplimiento_rutina_inventario": "Cumplimiento Rutina De Inventario",
        "arribo_fuera_plazo": "Arribo Fuera De Plazo",
        "arribo_total": "Arribo Total",
        "cumplimiento_arribo_repuestos": "Cumplimiento Arribo De Repuestos",
        "real_ubicacion_repuestos": "Real Ubicacion De Repuestos",
        "meta_ubicacion_repuestos": "Meta Ubicacion De Repuestos",
        "cumplimiento_ubicacion_repuestos": "Cumplimiento Ubicación De Repuestos",
        "cumplimiento_admin_stock": "Cumplimiento Administracion Stock",
        "pago_admin_stock": "Pago Administracion Stock",
        "no_retornos": "No Retornos",
        "pago_retornos": "Pago Retornos",
        "real_disponibilidad_flota": "Real Disponibilidad De Flota",
        "meta_disponibilidad_flota": "Meta Disponibilidad De Flota",
        "cumplimiento_disponibilidad_flota": "Cumplimiento Disponibilidad De Flota",
        "pago_disponibilidad_flota": "Pago Disponibilidad De Flota",
        "cumplimiento_venta": "Cumplimiento Venta",
        "pago_cumplimiento_venta": "Pago Cumplimiento Venta",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Diferencia Inventario", "Total Inventario",
        "% Ajuste De Inventario", "Cumplimiento Ajuste De Inventario",
        "Inventario Rotativo Pendiente", "Inventario Rotativo Total",
        "Cumplimiento Rutina De Inventario",
        "Arribo Fuera De Plazo", "Arribo Total", "Cumplimiento Arribo De Repuestos",
        "Real Ubicacion De Repuestos", "Meta Ubicacion De Repuestos",
        "Cumplimiento Ubicación De Repuestos",
        "Cumplimiento Administracion Stock", "Pago Administracion Stock",
        "No Retornos", "Pago Retornos",
        "Real Disponibilidad De Flota", "Meta Disponibilidad De Flota",
        "Cumplimiento Disponibilidad De Flota", "Pago Disponibilidad De Flota",
        "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Diferencia Inventario": "money",
        "Total Inventario": "money",
        "% Ajuste De Inventario": "percentage",
        "Cumplimiento Ajuste De Inventario": "percentage",
        "Inventario Rotativo Pendiente": "number",
        "Inventario Rotativo Total": "number",
        "Cumplimiento Rutina De Inventario": "percentage",
        "Arribo Fuera De Plazo": "number",
        "Arribo Total": "number",
        "Cumplimiento Arribo De Repuestos": "percentage",
        "Real Ubicacion De Repuestos": "number",
        "Meta Ubicacion De Repuestos": "number",
        "Cumplimiento Ubicación De Repuestos": "percentage",
        "Cumplimiento Administracion Stock": "percentage",
        "Pago Administracion Stock": "money",
        "No Retornos": "number",
        "Pago Retornos": "money",
        "Real Disponibilidad De Flota": "percentage",
        "Meta Disponibilidad De Flota": "percentage",
        "Cumplimiento Disponibilidad De Flota": "percentage",
        "Pago Disponibilidad De Flota": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': ["cargo2 contains 'supervisor cws'"],
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
            .str.contains("supervisor cws", na=False)
        )

        filtered_df = df[mask].copy()

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        logger.info(f"Filtered by cargo2 'supervisor cws': {len(filtered_df)} employees")

        return filtered_df, diagnostics

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_inventory_data(result)
        result = self._calculate_inventory_adjustment_compliance(result)
        result = self._calculate_inventory_routine_compliance(result)
        result = self._extract_arrival_data(result)
        result = self._calculate_arrival_compliance(result)
        result = self._extract_location_data(result)
        result = self._calculate_location_compliance(result)
        result = self._calculate_admin_stock_compliance(result)
        result = self._calculate_admin_stock_payment(result)
        result = self._extract_returns_data(result)
        result = self._calculate_returns_payment(result)
        result = self._extract_fleet_data(result)
        result = self._calculate_fleet_compliance(result)
        result = self._calculate_fleet_payment(result)
        result = self._extract_sales_data(result)
        result = self._calculate_sales_payment(result)
        result = self._calculate_total_commission(result)

        return result

    def _extract_inventory_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        dif_col = self._find_column(result, "diferencia inventario", "diferencia_inventario")
        tot_col = self._find_column(result, "total inventario", "total_inventario")
        pct_col = self._find_column(result, "% ajuste de inventario", "pct_ajuste")
        pend_col = self._find_column(result, "inventario rotativo pendiente", "inv_rotativo_pend")
        rot_tot_col = self._find_column(result, "inventario rotativo total", "inv_rotativo_total")

        result["diferencia_inventario"] = self._safe_numeric(result, dif_col)
        result["total_inventario"] = self._safe_numeric(result, tot_col)

        pct_values = self._safe_numeric(result, pct_col)
        if pct_values.max() > 2:
            pct_values = pct_values / 100
        result["pct_ajuste_inventario"] = pct_values

        result["inventario_rotativo_pendiente"] = self._safe_numeric(result, pend_col)
        result["inventario_rotativo_total"] = self._safe_numeric(result, rot_tot_col)

        return result

    def _calculate_inventory_adjustment_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["cumplimiento_ajuste_inventario"] = np.where(
            result["pct_ajuste_inventario"] < 0.01,
            1.0,
            np.minimum(0.9999999 / result["pct_ajuste_inventario"], 1.0)
        )

        return result

    def _calculate_inventory_routine_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["cumplimiento_rutina_inventario"] = np.where(
            result["inventario_rotativo_total"] > 0,
            1 - (result["inventario_rotativo_pendiente"] / result["inventario_rotativo_total"]),
            1.0
        )

        result["cumplimiento_rutina_inventario"] = np.clip(
            result["cumplimiento_rutina_inventario"], 0, 1
        )

        return result

    def _extract_arrival_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        fuera_col = self._find_column(result, "arribo fuera de plazo", "arribo_fuera")
        total_col = self._find_column(result, "arribo total", "arribo_total")

        result["arribo_fuera_plazo"] = self._safe_numeric(result, fuera_col)
        result["arribo_total"] = self._safe_numeric(result, total_col)

        return result

    def _calculate_arrival_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        arribo_fuera = result["arribo_fuera_plazo"]
        arribo_total = result["arribo_total"]

        cumplimiento = np.where(
            arribo_fuera <= 1,
            1.0,
            1 - (arribo_fuera / arribo_total)
        )

        cumplimiento = np.where(~np.isfinite(cumplimiento), 1.0, cumplimiento)
        result["cumplimiento_arribo_repuestos"] = np.clip(cumplimiento, 0, 1)

        return result

    def _extract_location_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        real_col = self._find_column(result, "real ubicacion de repuestos", "real_ubicacion")
        meta_col = self._find_column(result, "meta ubicacion de repuestos", "meta_ubicacion")

        result["real_ubicacion_repuestos"] = self._safe_numeric(result, real_col)
        result["meta_ubicacion_repuestos"] = self._safe_numeric(result, meta_col)

        return result

    def _calculate_location_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        real = result["real_ubicacion_repuestos"]
        meta = result["meta_ubicacion_repuestos"]

        cumpl = np.where(meta > 0, real / meta, 1.0)
        cumpl = np.where(cumpl > 0.97, 1.0, cumpl)
        cumpl = np.where(~np.isfinite(cumpl), 1.0, cumpl)

        result["cumplimiento_ubicacion_repuestos"] = np.clip(cumpl, 0, 1)

        return result

    def _calculate_admin_stock_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance_cols = [
            "cumplimiento_ajuste_inventario",
            "cumplimiento_rutina_inventario",
            "cumplimiento_arribo_repuestos",
            "cumplimiento_ubicacion_repuestos"
        ]

        for col in compliance_cols:
            if col not in result.columns:
                result[col] = 0

        comp_df = result[compliance_cols].fillna(0)
        count_100 = (comp_df >= 0.9999).sum(axis=1)

        result["cumplimiento_admin_stock"] = np.where(
            count_100 >= 2,
            comp_df.mean(axis=1),
            0.0
        )

        return result

    def _calculate_admin_stock_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_admin_stock"] = result["cumplimiento_admin_stock"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.ADMIN_STOCK_THRESHOLDS)
        )

        return result

    def _extract_returns_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        retornos_col = self._find_column(result, "no retornos", "no_retornos")
        result["no_retornos"] = self._safe_numeric(result, retornos_col)

        return result

    def _calculate_returns_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_retornos"] = np.where(
            result["no_retornos"] <= 0.99,
            100000,
            0
        )

        return result

    def _extract_fleet_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        real_col = self._find_column(result, "disponibilidad_flota", "real disponibilidad")
        meta_col = self._find_column(result, "meta disponibilidad", "meta_disponibilidad")

        if not meta_col:
            for col in result.columns:
                if col.lower().strip() == "meta":
                    meta_col = col
                    break

        real_values = self._safe_numeric(result, real_col)
        meta_values = self._safe_numeric(result, meta_col)

        if real_values.max() > 2:
            real_values = real_values / 100
        if meta_values.max() > 2:
            meta_values = meta_values / 100

        result["real_disponibilidad_flota"] = real_values
        result["meta_disponibilidad_flota"] = meta_values

        return result

    def _calculate_fleet_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["cumplimiento_disponibilidad_flota"] = np.where(
            result["meta_disponibilidad_flota"] > 0,
            np.minimum(
                result["real_disponibilidad_flota"] / result["meta_disponibilidad_flota"],
                1.0
            ),
            0.0
        )

        return result

    def _calculate_fleet_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_disponibilidad_flota"] = result["cumplimiento_disponibilidad_flota"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.FLEET_AVAILABILITY_THRESHOLDS)
        )

        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        cumpl_col = self._find_column(result, "cumplimiento venta", "cumplimiento_venta")
        cumpl_values = self._safe_numeric(result, cumpl_col)

        if cumpl_values.max() > 2:
            cumpl_values = cumpl_values / 100

        result["cumplimiento_venta"] = cumpl_values

        return result

    def _calculate_sales_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_cumplimiento_venta"] = result["cumplimiento_venta"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.SALES_COMPLIANCE_THRESHOLDS)
        )

        return result

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
            result["pago_admin_stock"].fillna(0) +
            result["pago_retornos"].fillna(0) +
            result["pago_disponibilidad_flota"].fillna(0) +
            result["pago_cumplimiento_venta"].fillna(0)
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

    def _safe_numeric(self, df: pd.DataFrame, col: str | None) -> pd.Series:
        if col and col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0)
        return pd.Series([0] * len(df), index=df.index)

    def _find_column(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None


SupervisorCWSStrategy = CWSSupervisorStrategy
