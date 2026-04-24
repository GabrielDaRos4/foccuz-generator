import logging
import time

from src.context.shared.infrastructure.retry import retry_on_rate_limit

logger = logging.getLogger(__name__)

API_THROTTLE_DELAY_SECONDS = 0.5

FORMAT_TYPES = {
    'money': ('CURRENCY', '"$"#,##0'),
    'currency': ('CURRENCY', '"$"#,##0.00'),
    'integer': ('NUMBER', '#,##0'),
    'number': ('NUMBER', '#,##0.00'),
    'decimal': ('NUMBER', '0.00'),
    'percentage': ('PERCENT', '0.00%'),
    'date': ('DATE', 'yyyy-mm-dd'),
}


class GSheetColumnFormatter:

    @staticmethod
    def apply(worksheet, columns: list, column_types: dict[str, str]) -> None:
        try:
            requests = GSheetColumnFormatter._build_format_requests(
                worksheet, columns, column_types
            )
            if requests:
                time.sleep(API_THROTTLE_DELAY_SECONDS)
                GSheetColumnFormatter._batch_update(worksheet, requests)
        except Exception as e:
            logger.warning(f"Error applying column formatting: {str(e)}")

    @staticmethod
    def _build_format_requests(worksheet, columns: list, column_types: dict[str, str]) -> list:
        requests = []

        for col_idx, col_name in enumerate(columns):
            col_type = column_types.get(col_name)
            if not col_type:
                continue

            format_info = FORMAT_TYPES.get(col_type)
            if not format_info:
                continue

            number_format_type, format_pattern = format_info
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': 1,
                        'startColumnIndex': col_idx,
                        'endColumnIndex': col_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': number_format_type,
                                'pattern': format_pattern
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            })

        return requests

    @staticmethod
    @retry_on_rate_limit()
    def _batch_update(worksheet, requests: list) -> None:
        worksheet.spreadsheet.batch_update({'requests': requests})
        logger.info(f"Applied formatting to {len(requests)} columns")
