import logging

import pandas as pd

logger = logging.getLogger(__name__)


class HistoricalLicensePlateAnalyzer:

    LICENSE_PLATE_COL = "patente"
    VOLUME_COL = "volumen_tct_premium"
    PRODUCT_COL = "producto"

    def __init__(self, product_type: str, months_lookback: int = 14):
        self._product_type = product_type.upper()
        self._months_lookback = months_lookback

    def analyze(self, historical_sales: list[pd.DataFrame]) -> tuple[set, set, set, pd.DataFrame, pd.DataFrame]:
        logger.info(f"Historical datasets available: {len(historical_sales)}")

        license_plates_m1 = self._extract_license_plates(historical_sales, 0)
        license_plates_m2 = self._extract_license_plates(historical_sales, 1)
        license_plates_historical = self._extract_historical_license_plates(historical_sales)

        df_m1 = self._extract_filtered_df(historical_sales, 0) if len(historical_sales) > 0 else pd.DataFrame()
        df_m2 = self._extract_filtered_df(historical_sales, 1) if len(historical_sales) > 1 else pd.DataFrame()

        return license_plates_m1, license_plates_m2, license_plates_historical, df_m1, df_m2

    def _extract_license_plates(self, historical: list, index: int) -> set[str]:
        if len(historical) <= index:
            return set()

        license_plates = self._extract_license_plates_from_df(historical[index])
        logger.info(f"M-{index + 1}: {len(license_plates)} unique license plates")
        return license_plates

    def _extract_historical_license_plates(self, historical: list) -> set[str]:
        if len(historical) < 3:
            return set()

        license_plates = set()
        for i in range(2, min(len(historical), self._months_lookback)):
            license_plates.update(self._extract_license_plates_from_df(historical[i]))

        logger.info(f"M-3 onwards: {len(license_plates)} unique license plates")
        return license_plates

    def _extract_license_plates_from_df(self, df: pd.DataFrame) -> set[str]:
        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()

        if self.PRODUCT_COL in df.columns:
            df = df[df[self.PRODUCT_COL].str.upper().str.strip() == self._product_type]

        if self.VOLUME_COL in df.columns:
            df[self.VOLUME_COL] = pd.to_numeric(df[self.VOLUME_COL], errors="coerce").fillna(0)
            df = df[df[self.VOLUME_COL] > 0]

        if self.LICENSE_PLATE_COL not in df.columns:
            return set()

        return set(df[self.LICENSE_PLATE_COL].astype(str).str.upper().str.strip().unique())

    def _extract_filtered_df(self, historical: list, index: int) -> pd.DataFrame:
        if len(historical) <= index:
            return pd.DataFrame()

        df = historical[index].copy()
        df.columns = df.columns.str.lower().str.strip()

        if self.PRODUCT_COL in df.columns:
            df = df[df[self.PRODUCT_COL].str.upper().str.strip() == self._product_type]

        if self.VOLUME_COL in df.columns:
            df[self.VOLUME_COL] = pd.to_numeric(df[self.VOLUME_COL], errors="coerce").fillna(0)
            df = df[df[self.VOLUME_COL] > 0]

        return df
