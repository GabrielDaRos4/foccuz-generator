import logging

import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class CWSManagerStrategy(BaseScaniaStrategy):

    STOCK_ADMIN_THRESHOLDS = [
        (100, float("inf"), 200000),
        (96, 99.99, 100000),
        (91, 95.99, 50000),
        (0, 90.99, 0),
    ]

    FLEET_AVAILABILITY_PAYMENT = 100000
    DEFAULT_OPEN_WO_PAYMENT = 150000
    MIN_COMPLIANCES_AT_100_PERCENT = 2
    COMPLIANCE_THRESHOLD = 0.9999
    PARTS_LOCATION_COMPLIANCE_THRESHOLD = 0.97

    COLUMN_RENAME_MAP = {
        "diferencia inventario": "Diferencia Inventario",
        "total inventario": "Total Inventario",
        "pct_ajuste_inventario": "% Ajuste Inventario",
        "inventory_adjustment_compliance": "Cumplimiento Ajuste Inventario",
        "inventario rotativo pendiente": "Inv Rotativo Pendiente",
        "inventario rotativo total": "Inv Rotativo Total",
        "inventory_routine_compliance": "Cumplimiento Rutina Inventario",
        "arribo fuera de plazo": "Arribo Fuera Plazo",
        "arribo total": "Arribo Total",
        "parts_arrival_compliance": "Cumplimiento Arribo Repuestos",
        "real ubicacion de repuestos": "Real Ubicación",
        "meta ubicacion de repuestos": "Meta Ubicación",
        "parts_location_compliance": "Cumplimiento Ubicación Repuestos",
        "stock_admin_compliance": "Cumplimiento Administración Stock",
        "stock_admin_payment": "Pago Administración Stock",
        "real disponibilidad de flota": "Real Disponibilidad",
        "meta disponibilidad de flota": "Meta Disponibilidad",
        "fleet_availability_payment": "Pago Disponibilidad Flota",
        "ot abiertas": "OT Abiertas",
        "open_work_orders_payment": "Pago OT Abiertas",
        "final_payment": "Pago Final",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Diferencia Inventario", "Total Inventario", "% Ajuste Inventario",
        "Cumplimiento Ajuste Inventario", "Inv Rotativo Pendiente", "Inv Rotativo Total",
        "Cumplimiento Rutina Inventario", "Arribo Fuera Plazo", "Arribo Total",
        "Cumplimiento Arribo Repuestos", "Real Ubicación", "Meta Ubicación",
        "Cumplimiento Ubicación Repuestos", "Cumplimiento Administración Stock",
        "Pago Administración Stock", "Real Disponibilidad", "Meta Disponibilidad",
        "Pago Disponibilidad Flota", "OT Abiertas", "Pago OT Abiertas",
        "Pago Final", "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Diferencia Inventario": "integer",
        "Total Inventario": "integer",
        "% Ajuste Inventario": "decimal",
        "Cumplimiento Ajuste Inventario": "percentage",
        "Cumplimiento Rutina Inventario": "percentage",
        "Cumplimiento Arribo Repuestos": "percentage",
        "Real Ubicación": "integer",
        "Meta Ubicación": "integer",
        "Cumplimiento Ubicación Repuestos": "percentage",
        "Cumplimiento Administración Stock": "percentage",
        "Pago Administración Stock": "money",
        "Real Disponibilidad": "decimal",
        "Meta Disponibilidad": "decimal",
        "Pago Disponibilidad Flota": "money",
        "OT Abiertas": "integer",
        "Pago OT Abiertas": "money",
        "Pago Final": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._convert_numeric_columns(result)
        result = self._calculate_individual_compliances(result)
        result = self._calculate_stock_admin_compliance(result)
        result = self._calculate_stock_admin_payment(result)
        result = self._calculate_fleet_availability_payment(result)
        result = self._calculate_open_work_orders_payment(result)
        result = self._calculate_final_payment(result)
        return result

    def _convert_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        numeric_cols = [
            "diferencia inventario", "total inventario", "pct_ajuste_inventario",
            "% ajuste de inventario", "inventario rotativo pendiente",
            "inventario rotativo total", "arribo fuera de plazo", "arribo total",
            "real ubicacion de repuestos", "meta ubicacion de repuestos",
            "real disponibilidad de flota", "meta disponibilidad de flota",
            "ot abiertas", "pago ot abiertas"
        ]
        for col in numeric_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)
        return result

    def _calculate_individual_compliances(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["inventory_adjustment_compliance"] = self._get_inventory_adjustment_compliance(result)
        result["inventory_routine_compliance"] = self._get_inventory_routine_compliance(result)
        result["parts_arrival_compliance"] = self._get_parts_arrival_compliance(result)
        result["parts_location_compliance"] = self._get_parts_location_compliance(result)
        return result

    def _get_inventory_adjustment_compliance(self, df: pd.DataFrame) -> pd.Series:
        pct_col = self._find_pct_adjustment_column(df)
        if pct_col is None:
            return pd.Series([0.0] * len(df), index=df.index)

        pct_values = df[pct_col]
        return np.where(
            pct_values < 0.01,
            np.where(pct_values > 0, 0.99999 / pct_values, 1.0),
            1.0
        )

    def _find_pct_adjustment_column(self, df: pd.DataFrame) -> str | None:
        for col in ["pct_ajuste_inventario", "% ajuste de inventario"]:
            if col in df.columns:
                return col
        return None

    def _get_inventory_routine_compliance(self, df: pd.DataFrame) -> pd.Series:
        pending_col = "inventario rotativo pendiente"
        if pending_col in df.columns:
            return (df[pending_col] == 0).astype(float)
        return pd.Series([0.0] * len(df), index=df.index)

    def _get_parts_arrival_compliance(self, df: pd.DataFrame) -> pd.Series:
        late_col = "arribo fuera de plazo"
        if late_col in df.columns:
            return (df[late_col] == 0).astype(float)
        return pd.Series([0.0] * len(df), index=df.index)

    def _get_parts_location_compliance(self, df: pd.DataFrame) -> pd.Series:
        actual_col = "real ubicacion de repuestos"
        target_col = "meta ubicacion de repuestos"

        if actual_col not in df.columns or target_col not in df.columns:
            return pd.Series([0.0] * len(df), index=df.index)

        ratio = np.where(df[target_col] > 0, df[actual_col] / df[target_col], 0)
        return np.where(ratio < self.PARTS_LOCATION_COMPLIANCE_THRESHOLD, ratio, 1.0)

    def _calculate_stock_admin_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        compliance_cols = [
            "inventory_adjustment_compliance",
            "inventory_routine_compliance",
            "parts_arrival_compliance",
            "parts_location_compliance"
        ]

        compliance_matrix = result[compliance_cols].values
        count_at_100 = (compliance_matrix >= self.COMPLIANCE_THRESHOLD).sum(axis=1)
        avg_compliance = compliance_matrix.mean(axis=1)

        result["stock_admin_compliance"] = np.where(
            count_at_100 >= self.MIN_COMPLIANCES_AT_100_PERCENT,
            avg_compliance,
            0.0
        )
        return result

    def _calculate_stock_admin_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["stock_admin_payment"] = result["stock_admin_compliance"].apply(
            lambda x: self.calculate_compliance_payment(x, self.STOCK_ADMIN_THRESHOLDS)
        )
        return result

    def _calculate_fleet_availability_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        actual_col = "real disponibilidad de flota"
        target_col = "meta disponibilidad de flota"

        if actual_col in result.columns and target_col in result.columns:
            result["fleet_availability_payment"] = np.where(
                result[actual_col] >= result[target_col],
                self.FLEET_AVAILABILITY_PAYMENT,
                0
            )
        else:
            result["fleet_availability_payment"] = 0
        return result

    def _calculate_open_work_orders_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        payment_col = "pago ot abiertas"
        if payment_col in result.columns:
            result["open_work_orders_payment"] = pd.to_numeric(
                result[payment_col], errors="coerce"
            ).fillna(self.DEFAULT_OPEN_WO_PAYMENT)
        else:
            result["open_work_orders_payment"] = self.DEFAULT_OPEN_WO_PAYMENT
        return result

    def _calculate_final_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["final_payment"] = (
            result["stock_admin_payment"] +
            result["fleet_availability_payment"] +
            result["open_work_orders_payment"]
        )
        result = self._apply_days_proration(result, "final_payment")
        return result


JefeCWSStrategy = CWSManagerStrategy
