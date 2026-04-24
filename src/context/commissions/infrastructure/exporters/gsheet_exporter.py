import logging
import os
import time

import pandas as pd

from src.context.commissions.domain.exceptions import ExportError
from src.context.commissions.domain.ports import Exporter
from src.context.commissions.domain.value_objects import OutputConfig
from src.context.commissions.infrastructure.exporters.dataframe_sanitizer import DataFrameSanitizer
from src.context.commissions.infrastructure.exporters.gsheet_column_formatter import GSheetColumnFormatter
from src.context.shared.infrastructure.retry import retry_on_rate_limit

logger = logging.getLogger(__name__)

API_THROTTLE_DELAY_SECONDS = 0.5

DEFAULT_CREDENTIAL_PATHS = [
    "credentials.json",
    "config/credentials.json",
    os.path.expanduser("~/.config/gcloud/application_default_credentials.json"),
]

GSHEET_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]


class GSheetExporter(Exporter):

    def __init__(self, credentials_path: str = None):
        self.credentials_path = self._resolve_credentials_path(credentials_path)
        self.client = None
        self._initialize_client()

    @staticmethod
    def _resolve_credentials_path(credentials_path: str = None) -> str:
        if credentials_path:
            return credentials_path

        env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        if env_path:
            return env_path

        for path in DEFAULT_CREDENTIAL_PATHS:
            if os.path.exists(path):
                return path

        return ""

    def _initialize_client(self) -> None:
        try:
            import gspread
            from google.oauth2.service_account import Credentials

            if self.credentials_path:
                credentials = Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=GSHEET_SCOPES
                )
                self.client = gspread.authorize(credentials)
                logger.info(f"Google Sheets client initialized with: {self.credentials_path}")
            else:
                logger.warning("No credentials found. Set GOOGLE_APPLICATION_CREDENTIALS or pass --credentials")

        except ImportError:
            logger.warning("gspread not installed. Google Sheets export will not work.")
        except Exception as e:
            logger.error(f"Error initializing Google Sheets client: {str(e)}")

    @retry_on_rate_limit()
    def export(
        self,
        data: pd.DataFrame,
        output_config: OutputConfig,
        plan_name: str = ""
    ) -> None:
        if self.client is None:
            raise ExportError("Google Sheets client not initialized")

        if data.empty:
            logger.warning(f"No data to export for plan {plan_name}")
            return

        try:
            logger.info(
                f"Exporting {len(data)} rows to sheet {output_config.sheet_id}, "
                f"tab '{output_config.tab_name}'"
            )

            spreadsheet = self._open_spreadsheet(output_config.sheet_id)
            worksheet = self._get_or_create_worksheet(
                spreadsheet, output_config, len(data), len(data.columns)
            )

            sanitized_data = DataFrameSanitizer.sanitize(data)
            values = [sanitized_data.columns.tolist()] + sanitized_data.values.tolist()
            self._update_worksheet(worksheet, values)

            column_types = data.attrs.get('column_types', {})
            if column_types:
                GSheetColumnFormatter.apply(worksheet, data.columns.tolist(), column_types)

            logger.info(
                f"Successfully exported {len(data)} rows to "
                f"'{output_config.tab_name}' in sheet {output_config.sheet_id}"
            )

        except ExportError:
            raise
        except Exception as e:
            logger.error(
                f"Error exporting to Google Sheets "
                f"(sheet: {output_config.sheet_id}, tab: {output_config.tab_name}): {str(e)}"
            )
            raise

    @retry_on_rate_limit()
    def _open_spreadsheet(self, sheet_id: str):
        return self.client.open_by_key(sheet_id)

    @retry_on_rate_limit()
    def _get_or_create_worksheet(self, spreadsheet, output_config: OutputConfig, rows: int, cols: int):
        try:
            worksheet = spreadsheet.worksheet(output_config.tab_name)
            if output_config.clear_before_write:
                time.sleep(API_THROTTLE_DELAY_SECONDS)
                worksheet.clear()
                logger.info(f"Cleared existing data in tab '{output_config.tab_name}'")
            return worksheet
        except Exception:
            worksheet = spreadsheet.add_worksheet(
                title=output_config.tab_name,
                rows=rows + 1,
                cols=cols
            )
            logger.info(f"Created new tab '{output_config.tab_name}'")
            return worksheet

    @retry_on_rate_limit()
    def _update_worksheet(self, worksheet, values: list) -> None:
        time.sleep(API_THROTTLE_DELAY_SECONDS)
        worksheet.update(values, value_input_option='RAW')
