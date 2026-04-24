import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_ENCODINGS = ['utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1']


def read_csv_file(
    file_path: str | Path,
    separator: str = ',',
    encoding: str = 'utf-8',
    dtype: type = str,
    keep_default_na: bool = False,
    fallback_encodings: list[str] | None = None
) -> pd.DataFrame:
    encodings_to_try = [encoding] + (fallback_encodings or DEFAULT_ENCODINGS)
    seen = set()
    encodings_to_try = [e for e in encodings_to_try if not (e in seen or seen.add(e))]

    for enc in encodings_to_try:
        try:
            df = pd.read_csv(
                file_path,
                sep=separator,
                encoding=enc,
                dtype=dtype or str,
                keep_default_na=keep_default_na
            )
            logger.debug(f"Successfully read {file_path} with encoding: {enc}")
            return df
        except UnicodeDecodeError:
            if enc == encodings_to_try[-1]:
                raise
            continue
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {str(e)}")
            raise

    raise ValueError(f"Could not read file {file_path} with any encoding")
