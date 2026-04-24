import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class WorkshopSupervisorStrategy(BaseScaniaStrategy):

    SALES_COMPLIANCE_THRESHOLDS = [
        (120, float("inf"), 750000),
        (115, 119.99, 650000),
        (110, 114.99, 550000),
        (105, 109.99, 450000),
        (100, 104.99, 400000),
        (0, 99.99, 0),
    ]

    PRODUCTIVITY_THRESHOLDS = [
        (100, float("inf"), 150000),
        (95, 99.99, 120000),
        (90, 94.99, 90000),
        (85, 89.99, 60000),
        (83, 84.99, 42000),
        (0, 82.99, 0),
    ]

    EFFICIENCY_THRESHOLDS = [
        (110, float("inf"), 180000),
        (105, 109.99, 150000),
        (100, 104.99, 120000),
        (95, 99.99, 90000),
        (90, 94.99, 60000),
        (0, 89.99, 0),
    ]

    NPS_FACTOR_THRESHOLDS = [
        (100, float("inf"), 0.30),
        (95, 99.99, 0.25),
        (90, 94.99, 0.20),
        (86, 89.99, 0.15),
        (80, 85.99, 0.00),
        (0, 79.99, 0.00),
    ]

    RETORNOS_MONTO_LIMIT = 2000000

    COLUMN_RENAME_MAP = {
        "meta": "Meta",
        "venta": "Venta",
        "cumplimiento venta": "Cumplimiento Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "resultado nps": "Resultado NPS",
        "nps_factor": "Cumplimiento NPS",
        "cumplimiento productividad": "Cumplimiento Productividad",
        "productivity_payment": "Pago Productividad",
        "cumplimiento eficiencia": "Cumplimiento Eficiencia",
        "efficiency_payment": "Pago Eficiencia",
        "n_retornos": "N_Retornos",
        "monto_retornos": "Monto_Retornos",
        "retornos_factor": "Cumplimiento Retornos",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Resultado NPS", "Cumplimiento NPS",
        "Cumplimiento Productividad", "Pago Productividad",
        "Cumplimiento Eficiencia", "Pago Eficiencia",
        "N_Retornos", "Monto_Retornos", "Cumplimiento Retornos",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Meta": "money",
        "Venta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Resultado NPS": "percentage",
        "Cumplimiento NPS": "percentage",
        "Cumplimiento Productividad": "percentage",
        "Pago Productividad": "money",
        "Cumplimiento Eficiencia": "percentage",
        "Pago Eficiencia": "money",
        "N_Retornos": "integer",
        "Monto_Retornos": "money",
        "Cumplimiento Retornos": "percentage",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        original_attrs = df.attrs.copy()
        result = df.copy()
        result.attrs = original_attrs
        result = self._extract_sales_data(result)
        result = self._calculate_sales_compliance_payment(result)
        result = self._extract_nps_data(result)
        result = self._calculate_nps_factor(result)
        result = self._calculate_productivity_payment(result)
        result = self._extract_efficiency_data(result)
        result = self._calculate_efficiency_payment(result)
        result = self._extract_retornos_data(result)
        result = self._calculate_retornos_factor(result)
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
        patterns = ["meta", "budget", "budget1"]
        for col in df.columns:
            col_lower = col.lower().replace("-", "").replace(" ", "")
            for pattern in patterns:
                if pattern in col_lower:
                    return col
        return None

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

    def _find_sales_compliance_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "productividad" in col_lower or "eficiencia" in col_lower or "nps" in col_lower:
                continue
            if "% cumplimiento" in col_lower:
                return col
            if "cumplimiento" in col_lower and "venta" in col_lower:
                return col
        for col in df.columns:
            col_lower = col.lower()
            if col_lower == "% cumplimiento" or col_lower == "%cumplimiento":
                return col
        return None

    def _calculate_sales_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["sales_compliance_payment"] = result["cumplimiento venta"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.SALES_COMPLIANCE_THRESHOLDS)
        )
        return result

    def _extract_nps_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        nps_col = self._find_nps_column(result)

        if nps_col:
            nps_values = pd.to_numeric(result[nps_col], errors="coerce").fillna(0)
            if nps_values.max() > 2:
                nps_values = nps_values / 100
            result["resultado nps"] = nps_values
        else:
            result["resultado nps"] = 0

        return result

    def _find_nps_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "nps" in col.lower() and "cumplimiento" not in col.lower():
                return col
        return None

    def _calculate_nps_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["nps_factor"] = result["resultado nps"].apply(
            lambda x: self._get_nps_factor(x * 100)
        )
        return result

    def _get_nps_factor(self, pct: float) -> float:
        if pd.isna(pct):
            return 0.0
        for lower, upper, factor in self.NPS_FACTOR_THRESHOLDS:
            if lower <= pct <= upper:
                return factor
        return 0.0

    def _calculate_productivity_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        productivity_col = self._find_productivity_column(result)

        if productivity_col:
            productivity = pd.to_numeric(result[productivity_col], errors="coerce").fillna(0)
            if productivity.max() > 2:
                productivity = productivity / 100
            result["cumplimiento productividad"] = productivity
            result["productivity_payment"] = productivity.apply(
                lambda x: self._get_threshold_payment(x * 100, self.PRODUCTIVITY_THRESHOLDS)
            )
        else:
            result["cumplimiento productividad"] = 0
            result["productivity_payment"] = 0

        return result

    def _find_productivity_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "productividad" in col.lower():
                return col
        return None

    def _extract_efficiency_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        efficiency_col = self._find_efficiency_column(result)

        if efficiency_col:
            efficiency = pd.to_numeric(result[efficiency_col], errors="coerce").fillna(0)
            if efficiency.max() > 2:
                efficiency = efficiency / 100
            result["cumplimiento eficiencia"] = efficiency
        else:
            result["cumplimiento eficiencia"] = 0

        return result

    def _find_efficiency_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "eficiencia" in col.lower():
                return col
        return None

    def _calculate_efficiency_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["efficiency_payment"] = result["cumplimiento eficiencia"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.EFFICIENCY_THRESHOLDS)
        )
        return result

    def _extract_retornos_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        secondary_arrays = df.attrs.get('secondary_arrays', {})
        retornos_df = secondary_arrays.get('Retornos Servicio')

        if retornos_df is not None and not retornos_df.empty:
            retornos_df = retornos_df.copy()
            retornos_df.columns = retornos_df.columns.str.lower().str.strip()

            if 'rutsupervisor' in retornos_df.columns:
                retornos_df['total'] = pd.to_numeric(retornos_df.get('total', 0), errors='coerce').fillna(0)

                retornos_agg = (
                    retornos_df.groupby('rutsupervisor', as_index=False)
                    .agg(
                        n_retornos=('ot', 'nunique'),
                        monto_retornos=('total', 'sum')
                    )
                )

                retornos_agg['rutsupervisor'] = (
                    retornos_agg['rutsupervisor']
                    .astype(str)
                    .str.replace(r'\.', '', regex=True)
                    .str.replace(r'-.*', '', regex=True)
                    .str.strip()
                )

                rut_col = next((c for c in result.columns if 'rut' in c.lower() and 'match' not in c.lower()), None)
                if rut_col:
                    result['_rut_merge'] = (
                        result[rut_col]
                        .astype(str)
                        .str.replace(r'\.', '', regex=True)
                        .str.replace(r'-.*', '', regex=True)
                        .str.strip()
                    )
                    retornos_agg = retornos_agg.rename(columns={'rutsupervisor': '_rut_merge'})
                    result = result.merge(retornos_agg, on='_rut_merge', how='left')
                    result = result.drop(columns=['_rut_merge'], errors='ignore')
                    logger.info(f"Merged retornos data: {result['n_retornos'].notna().sum()} matches")

        if 'n_retornos' not in result.columns:
            result['n_retornos'] = 0
        if 'monto_retornos' not in result.columns:
            result['monto_retornos'] = 0

        result['n_retornos'] = pd.to_numeric(result['n_retornos'], errors='coerce').fillna(0)
        result['monto_retornos'] = pd.to_numeric(result['monto_retornos'], errors='coerce').fillna(0)

        return result

    def _find_column_by_pattern(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for col in df.columns:
            col_lower = col.lower().replace(" ", "").replace("_", "")
            for pattern in patterns:
                pattern_clean = pattern.lower().replace(" ", "").replace("_", "")
                if pattern_clean in col_lower:
                    return col
        return None

    def _calculate_retornos_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["retornos_factor"] = result.apply(
            lambda row: self._get_retornos_factor(row["n_retornos"], row["monto_retornos"]),
            axis=1
        )
        return result

    def _get_retornos_factor(self, n_retornos: float, monto_retornos: float) -> float:
        try:
            n = float(n_retornos) if not pd.isna(n_retornos) else 0
            monto = float(monto_retornos) if not pd.isna(monto_retornos) else 0
        except (TypeError, ValueError):
            return 1.0

        if monto >= self.RETORNOS_MONTO_LIMIT:
            return 0.0
        elif n <= 1:
            return 1.0
        elif n == 2:
            return 0.5
        else:
            return 0.0

    def _get_threshold_payment(self, pct: float, thresholds: list) -> int:
        if pd.isna(pct):
            return 0
        for lower, upper, payment in thresholds:
            if lower <= pct <= upper:
                return payment
        return 0

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        base_payment = (
            result["sales_compliance_payment"].fillna(0) +
            result["productivity_payment"].fillna(0) +
            result["efficiency_payment"].fillna(0)
        )

        nps_multiplier = 1 + result["nps_factor"].fillna(0)
        retornos_multiplier = result["retornos_factor"].fillna(1)

        result["commission"] = (base_payment * nps_multiplier * retornos_multiplier).round(0).astype(int)

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


SupervisorTallerStrategy = WorkshopSupervisorStrategy
