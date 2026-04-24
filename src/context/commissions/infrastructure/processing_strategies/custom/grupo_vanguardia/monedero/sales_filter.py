import logging

import pandas as pd

from .brand import Brand
from .brand_classifier import BrandClassifier

logger = logging.getLogger(__name__)


class SalesFilter:

    def __init__(self, brand_classifier: BrandClassifier):
        self._classifier = brand_classifier

    def filter_by_brand(self, df: pd.DataFrame, target_brand: Brand) -> pd.DataFrame:
        brand_col = self._find_column(df.columns, "brand")

        if not brand_col:
            raise ValueError("No 'brand' column found")

        brand_name = target_brand.value.upper()
        filtered = df[df[brand_col].str.upper().str.strip() == brand_name].copy()

        logger.info(f"Filtered by {target_brand.value}: {len(filtered)} records")
        return filtered

    def filter_delivered(self, df: pd.DataFrame) -> pd.DataFrame:
        status_col = self._find_column(df.columns, "status")

        if not status_col:
            logger.warning("No 'status' column found")
            return df

        filtered = df[df[status_col].str.lower().str.strip() == "entregado"].copy()
        logger.info(f"Filtered delivered: {len(filtered)} records")
        return filtered

    def filter_by_period(
            self,
            df: pd.DataFrame,
            year: int,
            month: int
    ) -> pd.DataFrame:
        date_col = self._find_date_column(df.columns)

        if not date_col:
            logger.warning("No date column found")
            return df

        df = df.copy()
        df["_date"] = pd.to_datetime(df[date_col], errors="coerce")

        filtered = df[
            (df["_date"].dt.year == year) &
            (df["_date"].dt.month == month)
            ].copy()

        logger.info(f"Filtered by period {year}-{month:02d}: {len(filtered)} records")
        return filtered

    def filter_by_consultant_brand(
            self,
            df: pd.DataFrame,
            target_brand: Brand
    ) -> pd.DataFrame:
        agency_col = self._find_column(df.columns, "agency")

        if not agency_col:
            logger.warning("No 'agency' column found")
            return df

        brand_name = target_brand.value.upper()
        filtered = df[df[agency_col].str.upper().str.contains(brand_name, na=False)].copy()

        logger.info(f"Filtered by consultant brand {brand_name}: {len(filtered)} records")
        return filtered

    @staticmethod
    def _find_column(columns: pd.Index, keyword: str) -> str | None:
        for col in columns:
            if keyword in col.lower():
                return col
        return None

    @staticmethod
    def _find_date_column(columns: pd.Index) -> str | None:
        patterns = [
            ("delivery", "date"),
            ("fecha", "entreg"),
            ("order", "date")
        ]

        for p1, p2 in patterns:
            for col in columns:
                col_lower = col.lower()
                if p1 in col_lower and p2 in col_lower:
                    return col

        return None
