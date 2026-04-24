import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ClaimsWarehouseOperatorStrategy(BaseScaniaStrategy):

    COMPLIANCE_THRESHOLDS = [
        (110, float("inf"), 300000),
        (100, 109.99, 200000),
        (90, 99.99, 100000),
        (0, 89.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "sales": "Venta",
        "target": "Meta",
        "sales_compliance": "Cumplimiento Venta",
        "commission_payment": "Pago Cumplimiento Venta",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._filter_employees_with_plan_data(result)
        if result.empty:
            return result

        result = self._extract_sales_data(result)
        result = self._calculate_compliance(result)
        result = self._calculate_commission_payment(result)
        result = self._apply_days_proration(result, "commission_payment")

        return result

    def _filter_employees_with_plan_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        compliance_col = next(
            (c for c in result.columns if "cumplimiento" in c.lower() or "compliance" in c.lower()),
            None
        )
        if compliance_col:
            has_data = result[compliance_col].notna() & (result[compliance_col] != 0)
            result = result[has_data].copy()
            logger.info(f"Filtered to {len(result)} employees with plan data")
        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        sales_col = next(
            (c for c in result.columns if "venta" in c or "actual" in c), None
        )
        target_col = next(
            (c for c in result.columns if "meta" in c or "budget" in c), None
        )

        result["sales"] = 0
        if sales_col:
            result["sales"] = pd.to_numeric(result[sales_col], errors="coerce").fillna(0)

        result["target"] = 0
        if target_col:
            result["target"] = pd.to_numeric(result[target_col], errors="coerce").fillna(0)

        return result

    def _calculate_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance_col = next(
            (c for c in result.columns if "cumplimiento" in c or "compliance" in c), None
        )

        if compliance_col:
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["sales_compliance"] = compliance
        elif result["target"].sum() > 0:
            result["sales_compliance"] = result["sales"] / result["target"].replace(0, 1)
        else:
            result["sales_compliance"] = 0

        return result

    def _calculate_commission_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["commission_payment"] = result["sales_compliance"].apply(
            lambda x: self.calculate_compliance_payment(x, self.COMPLIANCE_THRESHOLDS)
        )
        return result


OperarioBodegaSiniestrosStrategy = ClaimsWarehouseOperatorStrategy
