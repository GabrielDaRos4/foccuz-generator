import logging

import pandas as pd

logger = logging.getLogger(__name__)

METRIC_TYPE_VOLUMEN = "volumen"
METRIC_TYPE_MARGEN = "margen"

MIN_COMPLIANCE_THRESHOLD = 0.9554

PRODUCT_BONUS_MAPPING_VOLUMEN = {
    "TCT": "TCT_Vol",
    "TAE": "TAE_Vol",
    "CE + AppCE": "CE_AppCE_Vol",
    "Bluemax Total": "Bluemax_Vol",
    "Lubricantes": "Lub_Vol",
}

PRODUCT_BONUS_MAPPING_MARGEN = {
    "TCT": "TCT_Mar",
    "TAE": "TAE_Mar",
    "Bluemax TCT": "Bluemax_Mar",
}

PRODUCT_BONUS_MAPPING_TCTP = {
    "TCTP": "TCTP",
}


class BonusLookup:

    def __init__(self, bonus_df: pd.DataFrame, metric_type: str):
        self._bonus_df = self._prepare_bonus_df(bonus_df)
        self._metric_type = metric_type
        self._product_mapping = self._get_product_mapping()

    @staticmethod
    def _prepare_bonus_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()

        if "RUT" not in df.columns:
            if len(df.columns) >= 12:
                df.columns = [
                    "RUT", "Nombre", "TCT_Vol", "TAE_Vol", "CE_AppCE_Vol",
                    "Bluemax_Vol", "Lub_Vol", "TCTP", "TCT_Mar", "TAE_Mar",
                    "Bluemax_Mar", "Total"
                ]

        df = df[df["RUT"].notna()]
        df = df[~df["RUT"].astype(str).str.contains("RUT|Unnamed", na=False)]

        df["RUT_normalized"] = df["RUT"].astype(str).str.replace(".", "", regex=False).str.strip().str.upper()

        return df

    def _get_product_mapping(self) -> dict:
        if self._metric_type == METRIC_TYPE_VOLUMEN:
            mapping = PRODUCT_BONUS_MAPPING_VOLUMEN.copy()
            mapping.update(PRODUCT_BONUS_MAPPING_TCTP)
            return mapping
        else:
            return PRODUCT_BONUS_MAPPING_MARGEN.copy()

    def lookup(self, rep_id: str, product: str, compliance: float | None) -> tuple[str, int]:
        if compliance is None or pd.isna(compliance):
            return "-", 0

        if compliance < MIN_COMPLIANCE_THRESHOLD:
            return "-", 0

        bonus_column = self._product_mapping.get(product)
        if not bonus_column:
            return "0", 0

        bonus_value = self._get_bonus_value(rep_id, bonus_column)

        if bonus_value is None or pd.isna(bonus_value) or bonus_value == 0:
            return "0", 0

        bonus_int = int(bonus_value)
        return str(bonus_int), bonus_int

    def _get_bonus_value(self, rep_id: str, bonus_column: str) -> float | None:
        if self._bonus_df.empty:
            return None

        if bonus_column not in self._bonus_df.columns:
            logger.warning(f"Bonus column {bonus_column} not found")
            return None

        rep_id_normalized = rep_id.replace(".", "").strip().upper()

        match = self._bonus_df[self._bonus_df["RUT_normalized"] == rep_id_normalized]

        if match.empty:
            logger.debug(f"No bonus found for rep_id {rep_id}")
            return None

        value = match[bonus_column].iloc[0]

        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def is_available(self) -> bool:
        return not self._bonus_df.empty
