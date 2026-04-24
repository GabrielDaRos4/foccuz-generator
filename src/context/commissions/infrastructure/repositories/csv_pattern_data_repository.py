import logging
from glob import glob
from pathlib import Path

import pandas as pd

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig
from src.context.shared.infrastructure.file_parsers import read_csv_file

logger = logging.getLogger(__name__)


class CSVPatternDataRepository(DataRepository):

    def get_data_for_plan(self, source: DataSourceConfig) -> dict[str, pd.DataFrame]:
        config = source.config
        path_pattern = config.get('path_pattern', '')
        separator = config.get('separator', ',')
        encoding = config.get('encoding', 'utf-8')
        sort_order = config.get('sort_order', 'desc')
        max_files = config.get('max_files', None)
        current_key = config.get('current_key', 'current')
        historical_key_prefix = config.get('historical_key_prefix', 'historical')

        if not path_pattern:
            raise ValueError("path_pattern is required for csv_pattern type")

        files = sorted(glob(path_pattern))

        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {path_pattern}")

        if sort_order == 'desc':
            files = files[::-1]

        if max_files:
            files = files[:max_files]

        logger.info(
            f"Pattern matched {len(files)} files, "
            f"loading with key '{current_key}' and prefix '{historical_key_prefix}_*'"
        )

        result = {current_key: read_csv_file(files[0], separator, encoding)}

        logger.info(f"Loaded {current_key}: {Path(files[0]).name} ({len(result[current_key])} rows)")

        for idx, file_path in enumerate(files[1:], start=1):
            key = f'{historical_key_prefix}_{idx}'
            result[key] = read_csv_file(file_path, separator, encoding)
            logger.info(f"Loaded {key}: {Path(file_path).name} ({len(result[key])} rows)")

        logger.info(f"Successfully loaded {len(result)} datasets from pattern")
        return result
