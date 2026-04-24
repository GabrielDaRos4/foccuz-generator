import logging
from pathlib import Path

import pandas as pd

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig

logger = logging.getLogger(__name__)


class ExcelDataRepository(DataRepository):

    def get_data_for_plan(self, source: DataSourceConfig) -> pd.DataFrame | dict[str, pd.DataFrame]:
        config = source.config
        path = config.get('path', '')
        sheets = config.get('sheets', [])
        header_row = config.get('header_row', 0)

        logger.info(f"Loading Excel data from path: {path}")

        path_obj = Path(path)

        if not path_obj.is_file():
            raise FileNotFoundError(f"Excel file not found: {path}")

        if path_obj.suffix.lower() not in ['.xlsx', '.xls']:
            raise ValueError(f"File is not an Excel file: {path}")

        xl = pd.ExcelFile(path)
        logger.info(f"Excel file has sheets: {xl.sheet_names}")

        result = {}

        if sheets:
            for sheet_config in sheets:
                sheet_name = sheet_config.get('name')
                sheet_key = sheet_config.get('key', sheet_name)
                sheet_header = sheet_config.get('header_row', header_row)

                if sheet_name not in xl.sheet_names:
                    logger.warning(f"Sheet '{sheet_name}' not found in Excel file")
                    continue

                df = pd.read_excel(xl, sheet_name=sheet_name, header=sheet_header)
                logger.info(f"Loaded sheet '{sheet_name}' as '{sheet_key}' with {len(df)} rows")
                result[sheet_key] = df
        else:
            for sheet_name in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sheet_name, header=header_row)
                logger.info(f"Loaded sheet '{sheet_name}' with {len(df)} rows")
                result[sheet_name] = df

        if len(result) == 1:
            return list(result.values())[0]

        return result
