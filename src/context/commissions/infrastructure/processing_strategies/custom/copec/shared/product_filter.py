import logging

import pandas as pd

logger = logging.getLogger(__name__)


class ProductFilter:

    PRODUCT_COL = "producto"
    DEFAULT_VOLUME_COL = "volumen"
    DEFAULT_DISCOUNT_COL = "descuento"

    def __init__(
        self,
        product_type: str | list[str],
        volume_col: str = None,
        discount_col: str = None
    ):
        if isinstance(product_type, list):
            self._product_types = [p.upper() for p in product_type]
        else:
            self._product_types = [product_type.upper()]
        self._volume_col = volume_col or self.DEFAULT_VOLUME_COL
        self._discount_col = discount_col or self.DEFAULT_DISCOUNT_COL

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.PRODUCT_COL not in df.columns:
            raise ValueError(f"Column '{self.PRODUCT_COL}' not found")

        product_col_normalized = df[self.PRODUCT_COL].str.upper().str.strip()
        filtered = df[product_col_normalized.isin(self._product_types)].copy()

        logger.info(f"Filtered by product {self._product_types}: {len(filtered)} records")
        return filtered

    def filter_positive_volume(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._prepare_numeric(df)
        filtered = df[df[self._volume_col] > 0].copy()
        filtered = self._normalize_column_names(filtered)
        logger.info(f"Filtered volume > 0: {len(filtered)} records")
        return filtered

    def _prepare_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in [self._volume_col, self._discount_col]:
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found")
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._volume_col != self.DEFAULT_VOLUME_COL:
            df = df.rename(columns={self._volume_col: self.DEFAULT_VOLUME_COL})
        if self._discount_col != self.DEFAULT_DISCOUNT_COL:
            df = df.rename(columns={self._discount_col: self.DEFAULT_DISCOUNT_COL})
        return df
