import logging
import os
import time
from functools import wraps

import pandas as pd

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_DELAY = 2
MAX_DELAY = 60
BACKOFF_FACTOR = 2


def retry_on_rate_limit(max_retries=MAX_RETRIES, initial_delay=INITIAL_DELAY):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_str = str(e).lower()
                    is_rate_limit = (
                        '429' in error_str or
                        'rate limit' in error_str or
                        'quota' in error_str or
                        'too many requests' in error_str or
                        'resource exhausted' in error_str
                    )

                    if not is_rate_limit or attempt == max_retries:
                        raise

                    last_exception = e
                    logger.warning(
                        f"Rate limit hit, attempt {attempt + 1}/{max_retries + 1}. "
                        f"Waiting {delay}s before retry..."
                    )
                    time.sleep(delay)
                    delay = min(delay * BACKOFF_FACTOR, MAX_DELAY)

            raise last_exception

        return wrapper
    return decorator

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    logger.warning("gspread not available - GSheet data source will not work")


class GSheetDataRepository(DataRepository):

    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]

    def __init__(self, credentials_path: str = None):
        self._credentials_path = self._resolve_credentials_path(credentials_path)
        self._client = None

    @staticmethod
    def _resolve_credentials_path(credentials_path: str = None) -> str:
        if credentials_path:
            return credentials_path

        env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        if env_path:
            return env_path

        default_paths = [
            "credentials.json",
            "config/credentials.json",
            "config/google_credentials.json",
            os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        ]
        for path in default_paths:
            if os.path.exists(path):
                return path

        return "credentials.json"

    def _get_client(self):
        if self._client is None:
            if not GSPREAD_AVAILABLE:
                raise ImportError("gspread library is required for GSheet data source")

            credentials = Credentials.from_service_account_file(
                self._credentials_path,
                scopes=self.SCOPES
            )
            self._client = gspread.authorize(credentials)

        return self._client

    def get_data_for_plan(self, source: DataSourceConfig) -> pd.DataFrame | dict[str, pd.DataFrame]:
        config = source.config
        sheet_id = config.get('sheet_id', '')
        tab_name = config.get('tab_name', '')
        key = config.get('key', tab_name)

        logger.info(f"Loading GSheet data from sheet_id: {sheet_id}, tab: {tab_name}")

        client = self._get_client()

        spreadsheet = self._open_spreadsheet(client, sheet_id)
        worksheet = self._get_worksheet(spreadsheet, tab_name)
        data = self._get_all_records(worksheet)

        df = pd.DataFrame(data)
        logger.info(f"Loaded tab '{tab_name}' as '{key}' with {len(df)} rows")

        return df

    @retry_on_rate_limit()
    @staticmethod
    def _open_spreadsheet(client, sheet_id: str):
        return client.open_by_key(sheet_id)

    @retry_on_rate_limit()
    @staticmethod
    def _get_worksheet(spreadsheet, tab_name: str):
        return spreadsheet.worksheet(tab_name)

    @retry_on_rate_limit()
    @staticmethod
    def _get_all_records(worksheet) -> list:
        return worksheet.get_all_records()
