import pandas as pd

GSHEET_MAX_CELL_LENGTH = 49000


class DataFrameSanitizer:

    @staticmethod
    def sanitize(data: pd.DataFrame) -> pd.DataFrame:
        result = data.copy()
        for col in result.columns:
            result[col] = result[col].apply(DataFrameSanitizer._sanitize_cell_value)
        return result

    @staticmethod
    def _sanitize_cell_value(value):
        import numpy as np

        if value is None:
            return ""
        if isinstance(value, np.ndarray):
            return _truncate(str(value.tolist()))
        try:
            if pd.isna(value):
                return ""
        except (ValueError, TypeError):
            pass
        if isinstance(value, (dict, list)):
            return _truncate(str(value))
        if isinstance(value, str) and len(value) > GSHEET_MAX_CELL_LENGTH:
            return value[:GSHEET_MAX_CELL_LENGTH]
        return value


def _truncate(value: str) -> str:
    if len(value) > GSHEET_MAX_CELL_LENGTH:
        return value[:GSHEET_MAX_CELL_LENGTH]
    return value
