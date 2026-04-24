import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from ..shared import RutBuilder
from .historical_license_plate_analyzer import HistoricalLicensePlateAnalyzer
from .license_plate_bonus_calculator import LicensePlateBonusCalculator
from .license_plate_bonus_config import LicensePlateBonusConfig
from .license_plate_classifier import LicensePlateClassifier
from .license_plate_output_formatter import LicensePlateOutputFormatter, extract_period

logger = logging.getLogger(__name__)


def _extract_license_plates(df: pd.DataFrame) -> set[str]:
    license_plate_col = "patente"
    if license_plate_col not in df.columns:
        return set()
    return set(df[license_plate_col].astype(str).str.upper().str.strip().unique())


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    return df


def _combine_months(df_m0: pd.DataFrame, df_m1: pd.DataFrame, df_m2: pd.DataFrame) -> pd.DataFrame:
    dfs = []

    if not df_m0.empty:
        df_m0 = df_m0.copy()
        df_m0["_month_offset"] = 0
        dfs.append(df_m0)

    if not df_m1.empty:
        df_m1 = df_m1.copy()
        df_m1["_month_offset"] = 1
        dfs.append(df_m1)

    if not df_m2.empty:
        df_m2 = df_m2.copy()
        df_m2["_month_offset"] = 2
        dfs.append(df_m2)

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined 3 months: {len(combined)} records")
    return combined


class TctPremiumBonusStrategy(ProcessingStrategy):

    PRODUCT_COL = "producto"
    VOLUME_COL = "volumen_tct_premium"

    def __init__(
        self,
        product_type: str = "TCT PREMIUM",
        months_lookback: int = 14,
        bonus_per_month: float = 15000,
        target_period: str = None,
        rep_id_filter: str = None
    ):
        self._product_type = product_type.upper()
        self._rut_builder = RutBuilder()
        self._historical_analyzer = HistoricalLicensePlateAnalyzer(product_type, months_lookback)
        self._license_plate_classifier = LicensePlateClassifier()
        self._bonus_calculator = LicensePlateBonusCalculator(LicensePlateBonusConfig(bonus_per_month))
        self._output_formatter = LicensePlateOutputFormatter()
        self._rep_id_filter = rep_id_filter
        self._target_period = target_period

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        historical = data.attrs.get("ventas_historicas", [])
        data = _normalize_columns(data)

        filtered_m0 = self._filter_product(data)
        if filtered_m0.empty:
            logger.warning("No data after product filter")
            return pd.DataFrame()

        filtered_m0 = self._filter_positive_volume(filtered_m0)
        if filtered_m0.empty:
            logger.warning("No data after volume filter")
            return pd.DataFrame()

        filtered_m0 = self._rut_builder.build(filtered_m0)

        period = self._get_period(filtered_m0)
        (
            license_plates_m1, license_plates_m2, license_plates_historical, df_m1, df_m2
        ) = self._historical_analyzer.analyze(historical)

        df_m1 = self._rut_builder.build(df_m1) if not df_m1.empty else df_m1
        df_m2 = self._rut_builder.build(df_m2) if not df_m2.empty else df_m2

        combined = _combine_months(filtered_m0, df_m1, df_m2)

        if self._rep_id_filter:
            combined = self._filter_by_rep_id(combined)
            if combined.empty:
                return pd.DataFrame()

        license_plates_m0 = _extract_license_plates(filtered_m0)
        logger.info(
            f"License plates M-0: {len(license_plates_m0)}, M-1: {len(license_plates_m1)}, "
            f"M-2: {len(license_plates_m2)}, Historical: {len(license_plates_historical)}"
        )

        new_license_plates = self._license_plate_classifier.classify(
            combined, license_plates_m1, license_plates_m2, license_plates_historical, period
        )

        if new_license_plates.empty:
            logger.warning("No new license plates found")
            return pd.DataFrame()

        result = self._bonus_calculator.calculate(new_license_plates)
        return self._output_formatter.format(result, period)

    def _filter_product(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.PRODUCT_COL not in df.columns:
            return df

        filtered = df[df[self.PRODUCT_COL].str.upper().str.strip() == self._product_type].copy()
        logger.info(f"Filtered by product {self._product_type}: {len(filtered)} records")
        return filtered

    def _filter_positive_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.VOLUME_COL not in df.columns:
            return df

        df[self.VOLUME_COL] = pd.to_numeric(df[self.VOLUME_COL], errors="coerce").fillna(0)
        filtered = df[df[self.VOLUME_COL] > 0].copy()
        logger.info(f"Filtered volume > 0: {len(filtered)} records")
        return filtered

    def _filter_by_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_id_str = str(self._rep_id_filter).zfill(10)
        filtered = df[df["ejecutivo"].astype(str).str.zfill(10) == rep_id_str].copy()
        logger.info(f"Filtered by Rep ID {rep_id_str}: {len(filtered)} records")
        return filtered

    def get_column_types(self) -> dict[str, str]:
        return self._output_formatter.COLUMN_TYPES

    def _get_period(self, df: pd.DataFrame) -> datetime:
        if self._target_period:
            return datetime.strptime(str(self._target_period), '%Y-%m-%d')
        return extract_period(df)
