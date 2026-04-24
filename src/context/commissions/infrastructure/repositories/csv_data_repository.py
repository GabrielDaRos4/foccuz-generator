import logging
from glob import glob
from pathlib import Path

import pandas as pd

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig
from src.context.shared.infrastructure.file_parsers import read_csv_file

logger = logging.getLogger(__name__)


class CSVDataRepository(DataRepository):

    def get_data_for_plan(self, source: DataSourceConfig) -> pd.DataFrame:
        config = source.config
        path = config.get('path', '')
        pattern = config.get('pattern', '*.csv')
        separator = config.get('separator', ',')
        encoding = config.get('encoding', 'utf-8')
        latest_only = config.get('latest_only', False)

        logger.info(f"Loading CSV data from path: {path}")

        path_obj = Path(path)

        if path_obj.is_file():
            logger.info(f"Loading single file: {path}")
            df = read_csv_file(path, separator, encoding)
            logger.info(f"Loaded {len(df)} rows from {path}")
            return df

        elif path_obj.is_dir():
            search_pattern = str(path_obj / pattern)
            files = sorted(glob(search_pattern))

            if not files:
                raise FileNotFoundError(f"No files found matching pattern: {search_pattern}")

            logger.info(f"Found {len(files)} files matching pattern: {pattern}")

            if latest_only:
                file_to_load = files[-1]
                logger.info(f"Loading latest file: {file_to_load}")
                df = read_csv_file(file_to_load, separator, encoding)
                logger.info(f"Loaded {len(df)} rows from {file_to_load}")
                return df
            else:
                dfs = []
                for file_path in files:
                    logger.info(f"Loading file: {file_path}")
                    df = read_csv_file(file_path, separator, encoding)
                    dfs.append(df)
                    logger.info(f"  Loaded {len(df)} rows")

                combined_df = pd.concat(dfs, ignore_index=True)
                logger.info(f"Combined {len(files)} files into {len(combined_df)} total rows")
                return combined_df
        else:
            raise ValueError(f"Path does not exist or is invalid: {path}")
