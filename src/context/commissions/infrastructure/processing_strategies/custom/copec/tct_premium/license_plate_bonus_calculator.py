import logging

import pandas as pd

from .license_plate_bonus_config import LicensePlateBonusConfig

logger = logging.getLogger(__name__)


class LicensePlateBonusCalculator:

    LICENSE_PLATE_COL = "license_plate_normalized"
    CLIENT_RUT_COL = "client_rut_complete"
    REP_ID_COL = "rep_id"

    def __init__(self, config: LicensePlateBonusConfig):
        self._config = config

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._deduplicate(df)
        df = self._calculate_bonus(df)
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(by=[self.CLIENT_RUT_COL, self.LICENSE_PLATE_COL], ascending=[True, True])
        df = df.drop_duplicates(subset=[self.CLIENT_RUT_COL, self.LICENSE_PLATE_COL], keep="first")
        logger.info(f"After deduplication: {len(df)} records")
        return df

    def _calculate_bonus(self, df: pd.DataFrame) -> pd.DataFrame:
        df["new_client_bonus"] = self._config.bonus_per_month
        df["new_client_bonus"] = df["new_client_bonus"].round(0).astype(int)

        total = df["new_client_bonus"].sum()
        logger.info(f"Total bonus: ${total:,.0f}")
        return df
