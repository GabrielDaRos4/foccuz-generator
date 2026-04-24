import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class RegionsOperatorStrategy(BaseScaniaStrategy):

    SALES_THRESHOLDS = [
        (110, float("inf"), 300000),
        (100, 109.99, 200000),
        (90, 99.99, 100000),
        (0, 89.99, 0),
    ]

    STOCK_ADMIN_THRESHOLDS = [
        (110, float("inf"), 300000),
        (100, 109.99, 200000),
        (90, 99.99, 100000),
        (0, 89.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "venta": "Venta",
        "meta": "Meta",
        "cumplimiento_venta": "Cumplimiento Venta",
        "pago_cumplimiento_venta": "Pago Cumplimiento Venta",
        "inventario_rotativo_pendiente": "Inventario Rotativo Pendiente",
        "inventario_rotativo_total": "Inventario Rotativo Total",
        "cumplimiento_rutina_inventario": "Cumplimiento Rutina De Inventario",
        "real_ubicacion_repuestos": "Real Ubicación De Repuestos",
        "meta_ubicacion_repuestos": "Meta Ubicación De Repuestos",
        "cumplimiento_ubicacion_repuestos": "Cumplimiento Ubicación De Repuestos",
        "diferencia_inventario": "Diferencia_Inventario",
        "total_inventario": "Total Inventario",
        "pct_ajuste_inventario": "% Ajuste De Inventario",
        "cumplimiento_ajuste_inventario": "Cumplimiento Ajuste De Inventario",
        "arribo_fuera_plazo": "Arribo Fuera De Plazo",
        "arribo_total": "Arribo Total",
        "cumplimiento_arribo_repuestos": "Cumplimiento Arribo De Repuestos",
        "cumplimiento_admin_stock": "Cumplimiento Administracion Stock",
        "pago_admin_stock": "Pago Administracion Stock",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Inventario Rotativo Pendiente", "Inventario Rotativo Total",
        "Cumplimiento Rutina De Inventario",
        "Real Ubicación De Repuestos", "Meta Ubicación De Repuestos",
        "Cumplimiento Ubicación De Repuestos",
        "Diferencia_Inventario", "Total Inventario",
        "% Ajuste De Inventario", "Cumplimiento Ajuste De Inventario",
        "Arribo Fuera De Plazo", "Arribo Total", "Cumplimiento Arribo De Repuestos",
        "Cumplimiento Administracion Stock", "Pago Administracion Stock",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Inventario Rotativo Pendiente": "number",
        "Inventario Rotativo Total": "number",
        "Cumplimiento Rutina De Inventario": "percentage",
        "Real Ubicación De Repuestos": "number",
        "Meta Ubicación De Repuestos": "number",
        "Cumplimiento Ubicación De Repuestos": "percentage",
        "Diferencia_Inventario": "money",
        "Total Inventario": "money",
        "% Ajuste De Inventario": "percentage",
        "Cumplimiento Ajuste De Inventario": "percentage",
        "Arribo Fuera De Plazo": "number",
        "Arribo Total": "number",
        "Cumplimiento Arribo De Repuestos": "percentage",
        "Cumplimiento Administracion Stock": "percentage",
        "Pago Administracion Stock": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': ["cargo2 contains 'ope. bodega regiones'"],
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
            .str.contains("ope. bodega regiones", na=False)
        )

        filtered_df = df[mask].copy()

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        logger.info(f"Filtered by cargo2 'ope. bodega regiones': {len(filtered_df)} employees")

        return filtered_df, diagnostics

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_sales_data(result)
        result = self._calculate_sales_compliance(result)
        result = self._extract_inventory_routine_data(result)
        result = self._extract_parts_location_data(result)
        result = self._extract_inventory_adjustment_data(result)
        result = self._extract_arrival_time_data(result)
        result = self._calculate_stock_admin_compliance(result)
        result = self._calculate_total_commission(result)

        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        actual_col = self._find_column(result, "actual-is", "actual_is", "venta")
        budget_col = self._find_column(result, "budget1-is", "budget1_is", "meta")

        result["venta"] = pd.to_numeric(
            result[actual_col] if actual_col else 0, errors="coerce"
        ).fillna(0)

        result["meta"] = pd.to_numeric(
            result[budget_col] if budget_col else 0, errors="coerce"
        ).fillna(0)

        return result

    def _calculate_sales_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        cumpl_col = self._find_column(result, "cumplimiento")
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

        result["pago_cumplimiento_venta"] = result["cumplimiento_venta"].apply(
            lambda x: self.calculate_compliance_payment(x, self.SALES_THRESHOLDS)
        )

        return result

    def _extract_inventory_routine_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        pend_col = self._find_column(result, "cumpinvrotpend", "inv_rot_pend")
        total_col = self._find_column(result, "cumpinvrottotal", "inv_rot_total")
        cumpl_col = self._find_column(result, "cump_rutina_inventarios", "rutina_inventario")

        result["inventario_rotativo_pendiente"] = pd.to_numeric(
            result[pend_col] if pend_col else 0, errors="coerce"
        ).fillna(0)

        result["inventario_rotativo_total"] = pd.to_numeric(
            result[total_col] if total_col else 0, errors="coerce"
        ).fillna(0)

        result["cumplimiento_rutina_inventario"] = pd.to_numeric(
            result[cumpl_col] if cumpl_col else 0, errors="coerce"
        ).fillna(0)

        if result["cumplimiento_rutina_inventario"].max() > 2:
            result["cumplimiento_rutina_inventario"] = result["cumplimiento_rutina_inventario"] / 100

        return result

    def _extract_parts_location_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        real_col = self._find_column(result, "ubicrptvale", "ubic_rpt_vale")
        meta_col = self._find_column(result, "ubicrpttotal", "ubic_rpt_total")
        cumpl_col = self._find_column(result, "cump_ubicacion_repuestos", "ubicacion_repuestos")

        result["real_ubicacion_repuestos"] = pd.to_numeric(
            result[real_col] if real_col else 0, errors="coerce"
        ).fillna(0)

        result["meta_ubicacion_repuestos"] = pd.to_numeric(
            result[meta_col] if meta_col else 0, errors="coerce"
        ).fillna(0)

        result["cumplimiento_ubicacion_repuestos"] = pd.to_numeric(
            result[cumpl_col] if cumpl_col else 0, errors="coerce"
        ).fillna(0)

        if result["cumplimiento_ubicacion_repuestos"].max() > 2:
            result["cumplimiento_ubicacion_repuestos"] = result["cumplimiento_ubicacion_repuestos"] / 100

        return result

    def _extract_inventory_adjustment_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        dif_col = self._find_column(result, "difinv", "dif_inv")
        tot_col = self._find_column(result, "totinv", "tot_inv", "total_inventario")
        pct_col = self._find_column(result, "diferencia_inventario")

        result["diferencia_inventario"] = pd.to_numeric(
            result[dif_col] if dif_col else 0, errors="coerce"
        ).fillna(0)

        result["total_inventario"] = pd.to_numeric(
            result[tot_col] if tot_col else 0, errors="coerce"
        ).fillna(0)

        result["pct_ajuste_inventario"] = pd.to_numeric(
            result[pct_col] if pct_col else 0, errors="coerce"
        ).fillna(0)

        if result["pct_ajuste_inventario"].max() > 2:
            result["pct_ajuste_inventario"] = result["pct_ajuste_inventario"] / 100

        result["cumplimiento_ajuste_inventario"] = result["pct_ajuste_inventario"].apply(
            self._calculate_inventory_adjustment_compliance
        )

        return result

    def _calculate_inventory_adjustment_compliance(self, pct: float) -> float:
        if pd.isna(pct):
            return 0
        pct_value = pct * 100 if pct <= 1 else pct
        if pct_value <= 1.5:
            return 1.1
        elif pct_value <= 3.0:
            return 1.0
        else:
            return 0.9

    def _extract_arrival_time_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        fuera_col = self._find_column(result, "ingrptfplazo", "ing_rpt_fplazo")
        total_col = self._find_column(result, "ingrpttotal", "ing_rpt_total")
        cumpl_col = self._find_column(result, "tiempo_arribo", "arribo")

        result["arribo_fuera_plazo"] = pd.to_numeric(
            result[fuera_col] if fuera_col else 0, errors="coerce"
        ).fillna(0)

        result["arribo_total"] = pd.to_numeric(
            result[total_col] if total_col else 0, errors="coerce"
        ).fillna(0)

        result["cumplimiento_arribo_repuestos"] = pd.to_numeric(
            result[cumpl_col] if cumpl_col else 0, errors="coerce"
        ).fillna(0)

        if result["cumplimiento_arribo_repuestos"].max() > 2:
            result["cumplimiento_arribo_repuestos"] = result["cumplimiento_arribo_repuestos"] / 100

        return result

    def _calculate_stock_admin_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance_columns = [
            "cumplimiento_rutina_inventario",
            "cumplimiento_ubicacion_repuestos",
            "cumplimiento_ajuste_inventario",
            "cumplimiento_arribo_repuestos"
        ]

        compliance_sum = sum(
            result[col].fillna(0) for col in compliance_columns if col in result.columns
        )
        count = len([col for col in compliance_columns if col in result.columns])

        result["cumplimiento_admin_stock"] = compliance_sum / count if count > 0 else 0

        result["pago_admin_stock"] = result["cumplimiento_admin_stock"].apply(
            lambda x: self.calculate_compliance_payment(x, self.STOCK_ADMIN_THRESHOLDS)
        )

        return result

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["commission"] = (
            result["pago_cumplimiento_venta"].fillna(0) +
            result["pago_admin_stock"].fillna(0)
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


OperarioRegionesStrategy = RegionsOperatorStrategy
