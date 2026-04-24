import logging

import pandas as pd

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig

logger = logging.getLogger(__name__)


class JSONDataRepository(DataRepository):

    def get_data_for_plan(self, source: DataSourceConfig) -> pd.DataFrame:
        config = source.config

        path = config.get('path')
        url = config.get('url')

        if not path and not url:
            raise ValueError("JSON configuration must include 'path' or 'url'")

        try:
            if url:
                logger.info(f"Fetching data from URL: {url}")
                df = pd.read_json(url)
            else:
                logger.info(f"Reading data from file: {path}")
                df = pd.read_json(path)

            logger.info(f"Loaded {len(df)} rows from JSON")
            return df

        except Exception as e:
            logger.error(f"Error reading JSON: {str(e)}")
            raise

