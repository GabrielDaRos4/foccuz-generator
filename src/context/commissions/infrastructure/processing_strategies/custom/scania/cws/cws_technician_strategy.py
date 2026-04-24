import logging

import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class CWSTechnicianStrategy(BaseScaniaStrategy):

    ABSENTEEISM_THRESHOLDS = [
        (0, 0.99, 50000),
        (1, float("inf"), 0),
    ]

    FLEET_COMPLIANCE_THRESHOLDS = [
        (97, float("inf"), 50000),
        (90, 96.99, 25000),
        (0, 89.99, 0),
    ]

    DEFAULT_MINIMUM_COMMISSION = 50000

    COLUMN_RENAME_MAP = {
        "absent_days": "N° Ausentismos",
        "absenteeism_payment": "Pago Ausentismo",
        "fleet_availability": "% Disponibilidad De Flota",
        "meta_disponibilidad": "Meta Disponibilidad De Flota",
        "fleet_compliance": "Cumplimiento Disponibilidad De Flota",
        "availability_payment": "Pago Disponibilidad De Flota",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "N° Ausentismos", "Pago Ausentismo",
        "% Disponibilidad De Flota", "Meta Disponibilidad De Flota",
        "Cumplimiento Disponibilidad De Flota", "Pago Disponibilidad De Flota",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "N° Ausentismos": "integer",
        "Pago Ausentismo": "money",
        "% Disponibilidad De Flota": "percentage",
        "Meta Disponibilidad De Flota": "percentage",
        "Cumplimiento Disponibilidad De Flota": "percentage",
        "Pago Disponibilidad De Flota": "money",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': ["cargo2 contains 'tecnico cws'"],
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
            .str.contains("tecnico cws", na=False)
        )

        filtered_df = df[mask].copy()

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        logger.info(f"Filtered by cargo2 'tecnico cws': {len(filtered_df)} employees")

        return filtered_df, diagnostics

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._calculate_absenteeism_payment(result)
        result = self._calculate_fleet_compliance(result)
        result = self._calculate_availability_payment(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _calculate_absenteeism_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        absent_col = self._find_absenteeism_column(result)

        if absent_col:
            absent_days = pd.to_numeric(result[absent_col], errors="coerce").fillna(0)
            result["absent_days"] = absent_days
            result["absenteeism_payment"] = absent_days.apply(
                self._get_absenteeism_payment
            )
        else:
            result["absent_days"] = 0
            result["absenteeism_payment"] = 50000

        return result

    def _find_absenteeism_column(self, df: pd.DataFrame) -> str | None:
        patterns = ["diasausente", "dias_ausente", "ausentismo", "dias ausente", "n° ausentismos"]
        for pattern in patterns:
            for col in df.columns:
                col_clean = col.lower().replace(" ", "").replace("°", "")
                pattern_clean = pattern.replace(" ", "").replace("°", "")
                if pattern_clean in col_clean:
                    return col
        return None

    def _get_absenteeism_payment(self, value: float) -> int:
        if pd.isna(value):
            return 50000
        for lower, upper, payment in self.ABSENTEEISM_THRESHOLDS:
            if lower <= value <= upper:
                return payment
        return 0

    def _calculate_fleet_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        avail_col = self._find_column(result, "disponibilidad_flota", "% disponibilidad")
        meta_col = self._find_column(result, "meta disponibilidad", "meta_disponibilidad")

        if not meta_col:
            for col in result.columns:
                if col.lower().strip() == "meta":
                    meta_col = col
                    break

        avail_values = self._safe_numeric(result, avail_col)
        meta_values = self._safe_numeric(result, meta_col)

        if avail_values.max() > 2:
            avail_values = avail_values / 100
        if meta_values.max() > 2:
            meta_values = meta_values / 100

        result["fleet_availability"] = avail_values
        result["meta_disponibilidad"] = meta_values

        result["fleet_compliance"] = np.where(
            meta_values > 0,
            avail_values / meta_values,
            0.0
        )

        return result

    def _calculate_availability_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["availability_payment"] = result["fleet_compliance"].apply(
            self._get_availability_payment
        )

        return result

    def _get_availability_payment(self, value: float) -> int:
        if pd.isna(value):
            return 0
        pct = value * 100
        for lower, upper, payment in self.FLEET_COMPLIANCE_THRESHOLDS:
            if lower <= pct <= upper:
                return payment
        return 0

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["commission"] = (
            result["absenteeism_payment"].fillna(0) +
            result["availability_payment"].fillna(0)
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

        result["guaranteed"] = result["guaranteed"].apply(
            lambda x: x if x > 0 else self.DEFAULT_MINIMUM_COMMISSION
        )

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


TecnicoCWSStrategy = CWSTechnicianStrategy
