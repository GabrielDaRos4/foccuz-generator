import logging

import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class AdminManagerStrategy(BaseScaniaStrategy):

    OVERDUE_AMOUNT_THRESHOLDS = [
        (-99.99, 5.99, 160000),
        (6.00, 6.99, 130000),
        (7.00, 7.99, 110000),
        (8.00, 8.99, 90000),
        (9.00, 9.99, 60000),
        (10.00, 10.99, 30000),
        (11.00, float("inf"), 0),
    ]

    INVENTORY_ROUTINE_THRESHOLD = 95.0
    INVENTORY_ROUTINE_PAYMENT = 40000

    IVA_LOSS_NO_PAYMENT = 80000
    IVA_LOSS_YES_PAYMENT = 0

    UNCOLLECTIBLE_THRESHOLDS = [
        (0, 1000000, 80000),
        (1000001, 2000000, 70000),
        (2000001, 3000000, 60000),
        (3000001, 4000000, 50000),
        (4000001, 5000000, 40000),
        (5000001, 6000000, 30000),
        (6000001, 7000000, 20000),
        (7000001, 8000000, 10000),
        (8000001, float("inf"), 0),
    ]

    CHECKS_NO_PAYMENT = 40000
    CHECKS_YES_PAYMENT = 0

    SPECIAL_EMPLOYEE_ID = 8864

    COLUMN_RENAME_MAP = {
        "% monto vencido > 30 dias": "% Monto Vencido > 30 Días",
        "overdue_amount_payment": "Pago Monto Vencido > 30 Días",
        "rutina inventarios": "Rutina Inventarios",
        "inventory_routine_payment": "Pago Rutina Inventarios",
        "perdida iva": "Pérdida IVA",
        "iva_loss_payment": "Pago Pérdida IVA",
        "incobrables 90 dias": "Incobrables 90 Días",
        "uncollectible_payment": "Pago Incobrables 90 Días",
        "cheques": "Cheques",
        "checks_payment": "Pago Cheques",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "% Monto Vencido > 30 Días", "Pago Monto Vencido > 30 Días",
        "Rutina Inventarios", "Pago Rutina Inventarios",
        "Pérdida IVA", "Pago Pérdida IVA",
        "Incobrables 90 Días", "Pago Incobrables 90 Días",
        "Cheques", "Pago Cheques",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "% Monto Vencido > 30 Días": "percentage",
        "Pago Monto Vencido > 30 Días": "money",
        "Rutina Inventarios": "percentage",
        "Pago Rutina Inventarios": "money",
        "Pérdida IVA": "money",
        "Pago Pérdida IVA": "money",
        "Incobrables 90 Días": "money",
        "Pago Incobrables 90 Días": "money",
        "Pago Cheques": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._enrich_from_secondary_arrays(result)
        result = self._calculate_overdue_amount_payment(result)
        result = self._calculate_inventory_routine_payment(result)
        result = self._calculate_iva_loss_payment(result)
        result = self._calculate_uncollectible_payment(result)
        result = self._calculate_checks_payment(result)
        result = self._apply_special_employee_rules(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _enrich_from_secondary_arrays(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        if not secondary_arrays:
            return result

        branch_col = self._find_branch_name_column(result)
        if not branch_col:
            logger.warning("No branch column found for secondary array merge")
            return result

        result["_branch_key"] = result[branch_col].astype(str).str.strip().str.lower()

        array_mappings = [
            ('Monto_Vencido_Mayor_a_30_dias', 'monto_vencido', 'monto_vencido'),
            ('Perdida_IVA_clientes', 'perdida_iva', 'perdida_iva'),
            ('Protestos_sin_codificacion_sin_analisis_de_creditos', 'protestos', 'protestos'),
            ('Cumplimiento_Inventario_rotativo', 'inventario', 'cumplimiento_inventario'),
            ('Deudores_Incobrables_Mayor_a_90_dias', 'incobrables', 'deudores_incobrables'),
        ]

        for array_name, pattern, target_col in array_mappings:
            array_df = secondary_arrays.get(array_name)
            if array_df is None or array_df.empty:
                continue

            array_df = array_df.copy()
            array_df.columns = array_df.columns.str.lower().str.strip()

            value_col = next(
                (c for c in array_df.columns if pattern in c.lower()),
                None
            )
            branch_src = next(
                (c for c in array_df.columns if c.lower() == 'branch'),
                None
            )

            if value_col and branch_src:
                sub = array_df[[branch_src, value_col]].copy()
                sub["_branch_key"] = sub[branch_src].astype(str).str.strip().str.lower()
                sub = sub.rename(columns={value_col: target_col})
                sub = sub[["_branch_key", target_col]].drop_duplicates(subset=["_branch_key"])

                result = result.merge(sub, on="_branch_key", how="left")
                logger.info(f"Enriched with {array_name}: {result[target_col].notna().sum()} matches")

        result = result.drop(columns=["_branch_key"], errors="ignore")
        return result

    def _find_branch_name_column(self, df: pd.DataFrame) -> str | None:
        return next((c for c in df.columns if c.lower() == 'branch'), None)

    def _calculate_overdue_amount_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        overdue_col = self._find_overdue_column(result)

        if overdue_col:
            overdue_pct = pd.to_numeric(result[overdue_col], errors="coerce").fillna(0)
            if overdue_pct.max() > 2:
                overdue_pct = overdue_pct / 100
            result["% monto vencido > 30 dias"] = overdue_pct
            result["overdue_amount_payment"] = overdue_pct.apply(self._get_overdue_payment)
        else:
            result["% monto vencido > 30 dias"] = 0
            result["overdue_amount_payment"] = 0

        return result

    def _find_overdue_column(self, df: pd.DataFrame) -> str | None:
        if "monto_vencido" in df.columns:
            return "monto_vencido"
        patterns = [
            ("monto_vencido", "30"),
            ("monto", "vencido"),
            ("% monto vencido", ""),
        ]
        for primary, secondary in patterns:
            for col in df.columns:
                col_lower = col.lower()
                if primary in col_lower and (not secondary or secondary in col_lower):
                    return col
        return None

    def _get_overdue_payment(self, pct: float) -> int:
        if pd.isna(pct):
            return 0
        pct_100 = pct * 100
        for lower, upper, payment in self.OVERDUE_AMOUNT_THRESHOLDS:
            if lower <= pct_100 <= upper:
                return payment
        return 0

    def _calculate_inventory_routine_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        routine_col = self._find_inventory_routine_column(result)

        if routine_col:
            routine_pct = pd.to_numeric(result[routine_col], errors="coerce").fillna(0)
            if routine_pct.max() > 2:
                routine_pct = routine_pct / 100
            result["rutina inventarios"] = routine_pct
            result["inventory_routine_payment"] = np.where(
                routine_pct * 100 >= self.INVENTORY_ROUTINE_THRESHOLD,
                self.INVENTORY_ROUTINE_PAYMENT,
                0
            )
        else:
            result["rutina inventarios"] = 0
            result["inventory_routine_payment"] = 0

        return result

    def _find_inventory_routine_column(self, df: pd.DataFrame) -> str | None:
        if "cumplimiento_inventario" in df.columns:
            return "cumplimiento_inventario"
        patterns = [
            "cumplimiento_inventario_rotativo",
            "cumplimiento inventario rotativo",
            "rutina inventario",
            "inventario rotativo",
        ]
        for pattern in patterns:
            for col in df.columns:
                if pattern in col.lower().replace("_", " "):
                    return col
        return None

    def _calculate_iva_loss_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        iva_col = self._find_iva_loss_column(result)

        if iva_col:
            result["perdida iva"] = result[iva_col]
            result["iva_loss_payment"] = result[iva_col].apply(self._get_iva_loss_payment)
        else:
            result["perdida iva"] = "No"
            result["iva_loss_payment"] = self.IVA_LOSS_NO_PAYMENT

        return result

    def _find_iva_loss_column(self, df: pd.DataFrame) -> str | None:
        if "perdida_iva" in df.columns:
            return "perdida_iva"
        patterns = [
            "perdida_iva",
            "perdida iva",
        ]
        for pattern in patterns:
            for col in df.columns:
                if pattern in col.lower().replace("_", " "):
                    return col
        return None

    def _get_iva_loss_payment(self, value) -> int:
        if pd.isna(value) or value == "" or value is None:
            return self.IVA_LOSS_NO_PAYMENT
        if isinstance(value, str):
            value = value.strip().lower()
        if value in ["si", "sí", "true", "1", 1]:
            return self.IVA_LOSS_YES_PAYMENT
        if value in ["no", "false", "0", 0]:
            return self.IVA_LOSS_NO_PAYMENT
        return self.IVA_LOSS_NO_PAYMENT

    def _calculate_uncollectible_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        uncollect_col = self._find_uncollectible_column(result)

        if uncollect_col:
            uncollect_amount = pd.to_numeric(result[uncollect_col], errors="coerce").fillna(0)
            result["incobrables 90 dias"] = uncollect_amount
            result["uncollectible_payment"] = uncollect_amount.apply(self._get_uncollectible_payment)
        else:
            result["incobrables 90 dias"] = 0
            result["uncollectible_payment"] = 0

        return result

    def _find_uncollectible_column(self, df: pd.DataFrame) -> str | None:
        if "deudores_incobrables" in df.columns:
            return "deudores_incobrables"
        patterns = [
            "deudores_incobrables",
            "deudores incobrables",
            "incobrable",
        ]
        for pattern in patterns:
            for col in df.columns:
                if pattern in col.lower().replace("_", " "):
                    return col
        return None

    def _get_uncollectible_payment(self, amount: float) -> int:
        if pd.isna(amount):
            return 0
        for lower, upper, payment in self.UNCOLLECTIBLE_THRESHOLDS:
            if lower <= amount <= upper:
                return payment
        return 0

    def _calculate_checks_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        checks_col = self._find_checks_column(result)

        if checks_col:
            result["cheques"] = result[checks_col]
            result["checks_payment"] = result[checks_col].apply(self._get_checks_payment)
        else:
            result["cheques"] = "No"
            result["checks_payment"] = self.CHECKS_NO_PAYMENT

        return result

    def _find_checks_column(self, df: pd.DataFrame) -> str | None:
        if "protestos" in df.columns:
            return "protestos"
        patterns = [
            "protestos",
            "protesto",
            "cheque",
        ]
        for pattern in patterns:
            for col in df.columns:
                if pattern in col.lower():
                    return col
        return None

    def _get_checks_payment(self, value) -> int:
        if pd.isna(value) or value == "" or value is None:
            return self.CHECKS_NO_PAYMENT
        if isinstance(value, str):
            value = value.strip().lower()
        if value in ["si", "sí", "true", "1", 1]:
            return self.CHECKS_YES_PAYMENT
        if value in ["no", "false", "0", 0]:
            return self.CHECKS_NO_PAYMENT
        return self.CHECKS_NO_PAYMENT

    def _apply_special_employee_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        id_col = self._find_employee_id_column(result)

        if id_col is None:
            return result

        employee_ids = pd.to_numeric(result[id_col], errors="coerce")
        is_special = employee_ids == self.SPECIAL_EMPLOYEE_ID

        if is_special.any():
            result.loc[is_special, "checks_payment"] = 0
            result.loc[is_special, "inventory_routine_payment"] = 0
            result["rutina inventarios"] = result["rutina inventarios"].astype(object)
            result["cheques"] = result["cheques"].astype(object)
            result.loc[is_special, "rutina inventarios"] = "N/A"
            result.loc[is_special, "cheques"] = "N/A"

        return result

    def _find_employee_id_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "id" in col.lower() and "empleado" in col.lower():
                return col
        return None

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["commission"] = (
            result["overdue_amount_payment"].fillna(0) +
            result["inventory_routine_payment"].fillna(0) +
            result["iva_loss_payment"].fillna(0) +
            result["uncollectible_payment"].fillna(0) +
            result["checks_payment"].fillna(0)
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


EncargadoAdmStrategy = AdminManagerStrategy
