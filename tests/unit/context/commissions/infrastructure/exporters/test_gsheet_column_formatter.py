from unittest.mock import MagicMock

from src.context.commissions.infrastructure.exporters.gsheet_column_formatter import (
    FORMAT_TYPES,
    GSheetColumnFormatter,
)


class TestGSheetColumnFormatter:

    def test_should_build_request_for_money_type(self):
        worksheet = MagicMock()
        worksheet.id = 0
        columns = ["Comision"]
        column_types = {"Comision": "money"}

        requests = GSheetColumnFormatter._build_format_requests(
            worksheet, columns, column_types
        )

        assert len(requests) == 1
        number_format = requests[0]['repeatCell']['cell']['userEnteredFormat']['numberFormat']
        assert number_format['type'] == 'CURRENCY'
        assert number_format['pattern'] == '"$"#,##0'

    def test_should_build_request_for_percentage_type(self):
        worksheet = MagicMock()
        worksheet.id = 0
        columns = ["Porcentaje"]
        column_types = {"Porcentaje": "percentage"}

        requests = GSheetColumnFormatter._build_format_requests(
            worksheet, columns, column_types
        )

        assert len(requests) == 1
        number_format = requests[0]['repeatCell']['cell']['userEnteredFormat']['numberFormat']
        assert number_format['type'] == 'PERCENT'
        assert number_format['pattern'] == '0.00%'

    def test_should_skip_columns_without_type(self):
        worksheet = MagicMock()
        worksheet.id = 0
        columns = ["Name", "Comision"]
        column_types = {"Comision": "money"}

        requests = GSheetColumnFormatter._build_format_requests(
            worksheet, columns, column_types
        )

        assert len(requests) == 1
        assert requests[0]['repeatCell']['range']['startColumnIndex'] == 1

    def test_should_skip_unknown_format_types(self):
        worksheet = MagicMock()
        worksheet.id = 0
        columns = ["Unknown"]
        column_types = {"Unknown": "nonexistent_type"}

        requests = GSheetColumnFormatter._build_format_requests(
            worksheet, columns, column_types
        )

        assert len(requests) == 0

    def test_should_build_requests_for_multiple_columns(self):
        worksheet = MagicMock()
        worksheet.id = 0
        columns = ["Monto", "Porcentaje", "Fecha"]
        column_types = {"Monto": "currency", "Porcentaje": "percentage", "Fecha": "date"}

        requests = GSheetColumnFormatter._build_format_requests(
            worksheet, columns, column_types
        )

        assert len(requests) == 3

    def test_should_have_all_expected_format_types(self):
        expected_types = {'money', 'currency', 'integer', 'number', 'decimal', 'percentage', 'date'}

        assert set(FORMAT_TYPES.keys()) == expected_types

    def test_apply_should_delegate_to_batch_update(self):
        worksheet = MagicMock()
        worksheet.id = 0
        spreadsheet = MagicMock()
        worksheet.spreadsheet = spreadsheet
        columns = ["Comision"]
        column_types = {"Comision": "money"}

        GSheetColumnFormatter.apply(worksheet, columns, column_types)

        spreadsheet.batch_update.assert_called_once()

    def test_apply_should_skip_when_no_requests(self):
        worksheet = MagicMock()
        worksheet.id = 0
        columns = ["Name"]
        column_types = {}

        GSheetColumnFormatter.apply(worksheet, columns, column_types)

        assert not hasattr(worksheet, 'spreadsheet') or not worksheet.spreadsheet.batch_update.called

    def test_apply_should_handle_exception_gracefully(self):
        worksheet = MagicMock()
        worksheet.id = 0
        spreadsheet = MagicMock()
        spreadsheet.batch_update.side_effect = RuntimeError("API error")
        worksheet.spreadsheet = spreadsheet
        columns = ["Comision"]
        column_types = {"Comision": "money"}

        GSheetColumnFormatter.apply(worksheet, columns, column_types)

    def test_batch_update_should_call_spreadsheet(self):
        worksheet = MagicMock()
        spreadsheet = MagicMock()
        worksheet.spreadsheet = spreadsheet
        requests = [{"repeatCell": {}}]

        GSheetColumnFormatter._batch_update(worksheet, requests)

        spreadsheet.batch_update.assert_called_once_with({"requests": requests})
