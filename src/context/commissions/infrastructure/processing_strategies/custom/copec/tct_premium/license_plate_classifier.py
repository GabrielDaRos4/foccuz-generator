import logging
from datetime import datetime

import pandas as pd

from .license_plate_classification import LicensePlateClassification

logger = logging.getLogger(__name__)


class LicensePlateClassifier:

    LICENSE_PLATE_COL = "patente"
    CLIENT_RUT_COL = "client_rut_complete"

    def classify(
        self,
        df: pd.DataFrame,
        license_plates_m1: set[str],
        license_plates_m2: set[str],
        license_plates_historical: set[str],
        period: datetime = None
    ) -> pd.DataFrame:
        df = df.copy()
        df["license_plate_normalized"] = df[self.LICENSE_PLATE_COL].astype(str).str.upper().str.strip()

        df["_classification"] = df["license_plate_normalized"].apply(
            lambda lp: self._classify_single(
                lp, license_plates_m1, license_plates_m2, license_plates_historical, period
            )
        )

        df["is_new_license_plate"] = df["_classification"].apply(lambda c: c.is_new)
        df["months_detail"] = df["_classification"].apply(lambda c: c.first_month_detail)

        new_license_plates = df[df["is_new_license_plate"]].copy()
        self._log_classification_results(new_license_plates)

        return new_license_plates

    @staticmethod
    def _classify_single(
        license_plate: str,
        license_plates_m1: set,
        license_plates_m2: set,
        license_plates_historical: set,
        period: datetime = None
    ) -> LicensePlateClassification:
        if license_plate in license_plates_historical:
            return LicensePlateClassification(is_new=False, first_month_offset=None, period=period)

        if license_plate in license_plates_m2:
            return LicensePlateClassification(is_new=True, first_month_offset=2, period=period)

        if license_plate in license_plates_m1:
            return LicensePlateClassification(is_new=True, first_month_offset=1, period=period)

        return LicensePlateClassification(is_new=True, first_month_offset=0, period=period)

    @staticmethod
    def _log_classification_results(df: pd.DataFrame) -> None:
        total = df["license_plate_normalized"].nunique()
        logger.info(f"New license plates: {total}")
