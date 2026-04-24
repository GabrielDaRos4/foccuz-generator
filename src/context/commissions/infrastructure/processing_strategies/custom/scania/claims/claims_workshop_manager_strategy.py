import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ClaimsWorkshopManagerStrategy(BaseScaniaStrategy):

    SALES_COMPLIANCE_THRESHOLDS = [
        (110, float("inf"), 520000),
        (105, 109.99, 420000),
        (100, 104.99, 320000),
        (95, 99.99, 220000),
        (90, 94.99, 120000),
        (0, 89.99, 0),
    ]

    PRODUCTIVITY_THRESHOLDS = [
        (100, float("inf"), 360000),
        (95, 99.99, 340000),
        (90, 94.99, 320000),
        (85, 89.99, 300000),
        (80, 84.99, 280000),
        (0, 79.99, 0),
    ]

    SPECIAL_EMPLOYEE_IDS = [5117]

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        filtered_df, diagnostics = super()._filter_by_role_with_diagnostics(df)

        id_col = self._find_employee_id_column(df)
        if id_col and self.SPECIAL_EMPLOYEE_IDS:
            employee_ids = pd.to_numeric(df[id_col], errors="coerce")
            special_mask = employee_ids.isin(self.SPECIAL_EMPLOYEE_IDS)
            special_employees = df[special_mask].copy()

            if not special_employees.empty:
                original_attrs = df.attrs.copy()
                filtered_df = pd.concat([filtered_df, special_employees], ignore_index=True)
                filtered_df = filtered_df.drop_duplicates(subset=[id_col])
                filtered_df.attrs = original_attrs
                logger.info(f"Added {len(special_employees)} special employees: {self.SPECIAL_EMPLOYEE_IDS}")

        return filtered_df, diagnostics

    def _find_employee_id_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "id" in col.lower() and "empleado" in col.lower():
                return col
        return None

    COLUMN_RENAME_MAP = {
        "meta": "Meta",
        "venta": "Venta",
        "cumplimiento venta": "Cumplimiento Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "cumplimiento productividad": "Cumplimiento Productividad",
        "productivity_payment": "Pago Productividad",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Meta", "Venta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Cumplimiento Productividad", "Pago Productividad",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Meta": "money",
        "Venta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Cumplimiento Productividad": "percentage",
        "Pago Productividad": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._extract_sales_data(result)
        result = self._calculate_sales_compliance_payment(result)
        result = self._extract_productivity_data(result)
        result = self._calculate_productivity_payment(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        target_col = self._find_target_column(result)
        if target_col:
            result["meta"] = pd.to_numeric(result[target_col], errors="coerce").fillna(0)
        else:
            result["meta"] = 0

        sales_col = self._find_sales_column(result)
        if sales_col:
            result["venta"] = pd.to_numeric(result[sales_col], errors="coerce").fillna(0)
        else:
            result["venta"] = 0

        compliance_col = self._find_sales_compliance_column(result)
        if compliance_col:
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["cumplimiento venta"] = compliance
        else:
            result["cumplimiento venta"] = 0

        return result

    def _find_target_column(self, df: pd.DataFrame) -> str | None:
        patterns = ["budget1", "budget", "meta"]
        for col in df.columns:
            col_lower = col.lower().replace("-", "").replace(" ", "")
            for pattern in patterns:
                if pattern in col_lower:
                    return col
        return None

    def _find_sales_column(self, df: pd.DataFrame) -> str | None:
        patterns = ["actual", "venta", "ventas"]
        for col in df.columns:
            col_lower = col.lower().replace("-", "").replace(" ", "")
            if "cumplimiento" in col_lower or "budget" in col_lower:
                continue
            for pattern in patterns:
                if pattern in col_lower:
                    return col
        return None

    def _find_sales_compliance_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "productividad" in col_lower:
                continue
            if "% cumplimiento" in col_lower or "cumplimiento" in col_lower:
                return col
        return None

    def _calculate_sales_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["sales_compliance_payment"] = result["cumplimiento venta"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.SALES_COMPLIANCE_THRESHOLDS)
        )
        return result

    def _extract_productivity_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        productivity_col = self._find_productivity_column(result)

        if productivity_col:
            productivity = pd.to_numeric(result[productivity_col], errors="coerce").fillna(0)
            if productivity.max() > 2:
                productivity = productivity / 100
            result["cumplimiento productividad"] = productivity
        else:
            result["cumplimiento productividad"] = 0

        return result

    def _find_productivity_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "productividad" in col.lower():
                return col
        return None

    def _calculate_productivity_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["productivity_payment"] = result["cumplimiento productividad"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.PRODUCTIVITY_THRESHOLDS)
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
            result["sales_compliance_payment"].fillna(0) +
            result["productivity_payment"].fillna(0)
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


JefeTallerSiniestroStrategy = ClaimsWorkshopManagerStrategy
