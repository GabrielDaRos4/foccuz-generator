
import pandas as pd


def validate_required_columns(data: pd.DataFrame, required_columns: list[str]) -> None:
    missing_columns: set[str] = set(required_columns) - set(data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
