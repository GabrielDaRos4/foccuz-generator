import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


INTERNAL_TO_POA_VOLUMEN = {
    "TCT": "TCT (M3)",
    "TAE": "TAE (M3)",
    "Bluemax Total": "BM (M3)",
    "Bluemax TCT": "BM (M3)",
    "Bluemax AppCE": "BM (M3)",
    "Lubricantes": "LUB (L)",
    "CE": "CE (M3)",
    "AppCE": "AppCE (M3)",
    "CE + AppCE": "CE + AppCE (M3)",
    "TCTP": "TCTP (N° Patentes)",
}

INTERNAL_TO_POA_MARGEN = {
    "TCT": "TCT ($/L)",
    "TAE": "TAE ($/L)",
    "Bluemax TCT": "BM ($/L)",
    "Bluemax AppCE": "BM ($/L)",
}


class PoaLookup:

    def __init__(self, poa_df: pd.DataFrame, metric_type: str, target_period: datetime):
        self._poa_df = self._prepare_poa_data(poa_df)
        self._metric_type = metric_type
        self._target_period = target_period
        self._period_column = self._find_period_column()
        self._internal_to_poa = (
            INTERNAL_TO_POA_VOLUMEN if metric_type == "volumen" else INTERNAL_TO_POA_MARGEN
        )

    def _prepare_poa_data(self, df: pd.DataFrame) -> pd.DataFrame | None:
        if df is None:
            return None
        df = df.copy()
        if "Rut" in df.columns:
            df["rut_sanitized"] = df["Rut"].astype(str).str.replace(".", "", regex=False)
        if "Producto" in df.columns:
            df["Producto"] = df["Producto"].apply(self._normalize_product_name)
        return df

    @staticmethod
    def _normalize_product_name(name: str) -> str:
        if not isinstance(name, str):
            return str(name)
        return name.replace("App CE", "AppCE").replace("CE + App CE", "CE + AppCE")

    def _find_period_column(self) -> str | None:
        if self._poa_df is None or self._poa_df.empty:
            return None

        for col in self._poa_df.columns:
            if isinstance(col, datetime):
                if col.year == self._target_period.year and col.month == self._target_period.month:
                    logger.info(f"Found POA period column: {col}")
                    return col

        logger.warning(f"No POA column found for period {self._target_period}")
        return None

    def lookup(self, rep_id: str, producto: str) -> float | None:
        if self._poa_df is None or self._poa_df.empty or self._period_column is None:
            return None

        poa_producto = self._get_poa_product_name(producto)
        if poa_producto is None:
            return None

        rut_sanitized = self._sanitize_rut(rep_id)

        mask = (
            (self._poa_df["rut_sanitized"] == rut_sanitized) &
            (self._poa_df["Producto"] == poa_producto)
        )

        matched = self._poa_df[mask]

        if matched.empty:
            return None

        poa_value = matched[self._period_column].iloc[0]

        if pd.isna(poa_value):
            return None

        return float(poa_value)

    def _get_poa_product_name(self, interno_product: str) -> str | None:
        return self._internal_to_poa.get(interno_product)

    @staticmethod
    def _sanitize_rut(rep_id: str) -> str:
        return rep_id.replace(".", "")

    def is_available(self) -> bool:
        return self._poa_df is not None and not self._poa_df.empty and self._period_column is not None
