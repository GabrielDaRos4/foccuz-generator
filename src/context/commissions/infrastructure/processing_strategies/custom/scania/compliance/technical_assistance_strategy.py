import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class TechnicalAssistanceStrategy(BaseScaniaStrategy):

    VISITS_THRESHOLDS = [
        (4, float("inf"), 150000),
        (3, 3.99, 112500),
        (2, 2.99, 75000),
        (1, 1.99, 37500),
        (0, 0.99, 0),
    ]

    CAMPAIGN_TC_THRESHOLDS = [
        (95, float("inf"), 125000),
        (81, 94.99, 93750),
        (71, 80.99, 62500),
        (50, 70.99, 31250),
        (0, 49.99, 0),
    ]

    WORK_PLAN_THRESHOLDS = [
        (54, float("inf"), 125000),
        (45, 53.99, 93750),
        (36, 44.99, 62500),
        (24, 35.99, 31250),
        (0, 23.99, 0),
    ]

    COLUMN_RENAME_MAP = {
        "real_plan_visitas": "Real Plan De Visitas",
        "pago_plan_visitas": "Pago Cumplimiento Plan De Visitas",
        "cumplimiento_campana_tc": "Cumplimiento Metas Campaña TC Y RC",
        "pago_campana_tc": "Pago Campaña TC Y RC",
        "cumplimiento_plan_trabajo": "Cumplimiento Plan De Trabajo",
        "pago_plan_trabajo": "Pago Cumplimiento Plan De Trabajo",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Real Plan De Visitas", "Pago Cumplimiento Plan De Visitas",
        "Cumplimiento Metas Campaña TC Y RC", "Pago Campaña TC Y RC",
        "Cumplimiento Plan De Trabajo", "Pago Cumplimiento Plan De Trabajo",
        "Comisión"
    ]

    COLUMN_TYPES = {
        "Real Plan De Visitas": "number",
        "Pago Cumplimiento Plan De Visitas": "money",
        "Cumplimiento Metas Campaña TC Y RC": "percentage",
        "Pago Campaña TC Y RC": "money",
        "Cumplimiento Plan De Trabajo": "percentage",
        "Pago Cumplimiento Plan De Trabajo": "money",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': ["cargo2 contains 'asistencia tecnica'"],
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
            .str.contains("asistencia tecnica", na=False)
        )

        filtered_df = df[mask].copy()

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        logger.info(f"Filtered by cargo2 'asistencia tecnica': {len(filtered_df)} employees")

        return filtered_df, diagnostics

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_visits_data(result)
        result = self._calculate_visits_payment(result)
        result = self._extract_campaign_tc_data(result)
        result = self._calculate_campaign_tc_payment(result)
        result = self._extract_work_plan_data(result)
        result = self._calculate_work_plan_payment(result)
        result = self._calculate_total_commission(result)

        return result

    def _extract_visits_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        visits_col = self._find_column(result, "programa_visitas", "real plan de visitas")

        if visits_col:
            result["real_plan_visitas"] = pd.to_numeric(
                result[visits_col], errors="coerce"
            ).fillna(0)
        else:
            result["real_plan_visitas"] = 0

        return result

    def _calculate_visits_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_plan_visitas"] = result["real_plan_visitas"].apply(
            lambda x: self._get_threshold_payment(x, self.VISITS_THRESHOLDS)
        )

        return result

    def _extract_campaign_tc_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        tc_col = self._find_column(
            result, "camp_tecnica_seguridad", "cumplimiento metas campaña tc"
        )

        if tc_col:
            result["cumplimiento_campana_tc"] = pd.to_numeric(
                result[tc_col], errors="coerce"
            ).fillna(0)
            if result["cumplimiento_campana_tc"].max() > 2:
                result["cumplimiento_campana_tc"] = result["cumplimiento_campana_tc"] / 100
        else:
            result["cumplimiento_campana_tc"] = 0

        return result

    def _calculate_campaign_tc_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_campana_tc"] = result["cumplimiento_campana_tc"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.CAMPAIGN_TC_THRESHOLDS)
        )

        return result

    def _extract_work_plan_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        work_col = self._find_column(
            result, "scania_assitance", "cumplimiento plan de trabajo"
        )

        if work_col:
            result["cumplimiento_plan_trabajo"] = pd.to_numeric(
                result[work_col], errors="coerce"
            ).fillna(0)
            if result["cumplimiento_plan_trabajo"].max() > 2:
                result["cumplimiento_plan_trabajo"] = result["cumplimiento_plan_trabajo"] / 100
        else:
            result["cumplimiento_plan_trabajo"] = 0

        return result

    def _calculate_work_plan_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["pago_plan_trabajo"] = result["cumplimiento_plan_trabajo"].apply(
            lambda x: self._get_threshold_payment(x * 100, self.WORK_PLAN_THRESHOLDS)
        )

        return result

    def _get_threshold_payment(self, value: float, thresholds: list) -> int:
        if pd.isna(value):
            return 0
        for lower, upper, payment in thresholds:
            if lower <= value <= upper:
                return payment
        return 0

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["commission"] = (
            result["pago_plan_visitas"].fillna(0) +
            result["pago_campana_tc"].fillna(0) +
            result["pago_plan_trabajo"].fillna(0)
        )

        return result

    def _find_column(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None


AsistenciaTecnicaStrategy = TechnicalAssistanceStrategy
