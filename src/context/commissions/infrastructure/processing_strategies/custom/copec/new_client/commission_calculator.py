import logging

import numpy as np
import pandas as pd

from .commission_config import CommissionConfig

logger = logging.getLogger(__name__)


class CommissionCalculator:

    VOLUME_COL = "volumen"
    DISCOUNT_COL = "descuento"
    CLIENT_RUT_COL = "client_rut_complete"

    def __init__(self, config: CommissionConfig):
        self._config = config

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._deduplicate(df)
        df = self._calculate_unit_discount(df)
        df = self._calculate_unit_commission(df)
        df = self._calculate_commission_amount(df)
        df = self._apply_bonus(df)
        df = self._calculate_total(df)
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(by=[self.CLIENT_RUT_COL, self.VOLUME_COL], ascending=[True, False])
        df = df.drop_duplicates(subset=[self.CLIENT_RUT_COL], keep="first")
        logger.info(f"After deduplication: {len(df)} records")
        return df

    def _calculate_unit_discount(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.DISCOUNT_COL] = df[self.DISCOUNT_COL] / df[self.VOLUME_COL]
        return df

    def _calculate_unit_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        df["unit_commission"] = np.maximum(
            self._config.max_factor - (df[self.DISCOUNT_COL] * self._config.discount_percentage),
            self._config.min_factor
        )
        return df

    def _calculate_commission_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        df["commission_amount"] = df["unit_commission"] * df[self.VOLUME_COL]
        return df

    def _apply_bonus(self, df: pd.DataFrame) -> pd.DataFrame:
        df["new_client_bonus"] = np.where(
            df["gets_bonus"],
            self._config.new_client_bonus,
            0
        )
        df["client_type"] = np.where(df["gets_bonus"], "SI", "-")
        return df

    @staticmethod
    def _calculate_total(df: pd.DataFrame) -> pd.DataFrame:
        df["total_commission"] = df["commission_amount"] + df["new_client_bonus"]
        df["total_commission"] = np.maximum(df["total_commission"], 0).round(0).astype(int)

        total = df["total_commission"].sum()
        logger.info(f"Total commission: ${total:,.0f}")
        return df
