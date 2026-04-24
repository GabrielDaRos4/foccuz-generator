import pandas as pd


def normalize_rut(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.replace(".", "", regex=False)
    )
    has_check_digit = series.astype(str).str.contains("-", regex=False)
    cleaned_no_dash = cleaned.str.replace("-", "", regex=False)
    return cleaned_no_dash.where(~has_check_digit, cleaned_no_dash.str[:-1])


def normalize_branch(series: pd.Series) -> pd.Series:
    normalized = series.astype(str).str.strip().str.upper()
    normalized = normalized.str.replace(r"\s+", " ", regex=True)
    normalized = normalized.str.replace("Ñ", "N", regex=False)
    normalized = normalized.str.replace("¤", "N", regex=False)
    normalized = normalized.str.replace(r"^CWS\s+", "", regex=True)
    return normalized
