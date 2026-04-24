from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.context.commissions.domain.exceptions import ExportError
from src.context.commissions.infrastructure.exporters.gsheet_exporter import GSheetExporter
from tests.mothers.commissions.domain.value_objects_mother import OutputConfigMother


@pytest.fixture
def mock_gspread():
    with patch("src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter._initialize_client"):
        exporter = GSheetExporter.__new__(GSheetExporter)
        exporter.credentials_path = "/fake/path"
        exporter.client = MagicMock()
        yield exporter


class TestGSheetExporterExport:

    def test_should_raise_when_client_not_initialized(self):
        _patch_init = (
            "src.context.commissions.infrastructure.exporters"
            ".gsheet_exporter.GSheetExporter._initialize_client"
        )
        with patch(_patch_init):
            exporter = GSheetExporter.__new__(GSheetExporter)
            exporter.credentials_path = ""
            exporter.client = None

        data = pd.DataFrame({"a": [1]})
        config = OutputConfigMother.default()

        with pytest.raises(ExportError, match="not initialized"):
            exporter.export(data, config)

    def test_should_skip_empty_data(self, mock_gspread):
        config = OutputConfigMother.default()

        mock_gspread.export(pd.DataFrame(), config)

        mock_gspread.client.open_by_key.assert_not_called()

    def test_should_export_data_successfully(self, mock_gspread):
        data = pd.DataFrame({"name": ["Alice", "Bob"], "amount": [100, 200]})
        config = OutputConfigMother.default()

        spreadsheet = MagicMock()
        worksheet = MagicMock()
        mock_gspread.client.open_by_key.return_value = spreadsheet
        spreadsheet.worksheet.return_value = worksheet

        mock_gspread.export(data, config, plan_name="Test Plan")

        mock_gspread.client.open_by_key.assert_called_once_with(config.sheet_id)
        worksheet.update.assert_called_once()

    def test_should_apply_formatting_when_column_types_present(self, mock_gspread):
        data = pd.DataFrame({"amount": [100.0]})
        data.attrs["column_types"] = {"amount": "money"}
        config = OutputConfigMother.default()

        spreadsheet = MagicMock()
        worksheet = MagicMock()
        worksheet.id = 0
        spreadsheet_prop = MagicMock()
        worksheet.spreadsheet = spreadsheet_prop
        mock_gspread.client.open_by_key.return_value = spreadsheet
        spreadsheet.worksheet.return_value = worksheet

        mock_gspread.export(data, config)

        spreadsheet_prop.batch_update.assert_called_once()

    def test_should_not_apply_formatting_when_no_column_types(self, mock_gspread):
        data = pd.DataFrame({"name": ["Alice"]})
        config = OutputConfigMother.default()

        spreadsheet = MagicMock()
        worksheet = MagicMock()
        mock_gspread.client.open_by_key.return_value = spreadsheet
        spreadsheet.worksheet.return_value = worksheet

        with patch(
            "src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetColumnFormatter.apply"
        ) as mock_fmt:
            mock_gspread.export(data, config)
            mock_fmt.assert_not_called()

    def test_should_reraise_export_error(self, mock_gspread):
        data = pd.DataFrame({"name": ["Alice"]})
        config = OutputConfigMother.default()
        mock_gspread.client.open_by_key.side_effect = ExportError("custom error")

        with pytest.raises(ExportError, match="custom error"):
            mock_gspread.export(data, config)

    def test_should_reraise_generic_exception(self, mock_gspread):
        data = pd.DataFrame({"name": ["Alice"]})
        config = OutputConfigMother.default()
        mock_gspread.client.open_by_key.side_effect = ConnectionError("network down")

        with pytest.raises(ConnectionError):
            mock_gspread.export(data, config)


class TestGSheetExporterWorksheet:

    def test_should_open_spreadsheet(self, mock_gspread):
        mock_gspread.client.open_by_key.return_value = MagicMock()

        mock_gspread._open_spreadsheet("sheet_id_123")

        mock_gspread.client.open_by_key.assert_called_once_with("sheet_id_123")

    def test_should_get_existing_worksheet(self, mock_gspread):
        spreadsheet = MagicMock()
        worksheet = MagicMock()
        spreadsheet.worksheet.return_value = worksheet
        config = OutputConfigMother.default()

        mock_gspread._get_or_create_worksheet(spreadsheet, config, 10, 5)

        spreadsheet.worksheet.assert_called_once_with(config.tab_name)

    def test_should_clear_worksheet_when_configured(self, mock_gspread):
        spreadsheet = MagicMock()
        worksheet = MagicMock()
        spreadsheet.worksheet.return_value = worksheet
        config = OutputConfigMother.default()

        mock_gspread._get_or_create_worksheet(spreadsheet, config, 10, 5)

        worksheet.clear.assert_called_once()

    def test_should_not_clear_when_append_mode(self, mock_gspread):
        spreadsheet = MagicMock()
        worksheet = MagicMock()
        spreadsheet.worksheet.return_value = worksheet
        config = OutputConfigMother.append_mode()

        mock_gspread._get_or_create_worksheet(spreadsheet, config, 10, 5)

        worksheet.clear.assert_not_called()

    def test_should_create_worksheet_when_not_found(self, mock_gspread):
        spreadsheet = MagicMock()
        spreadsheet.worksheet.side_effect = Exception("Worksheet not found")
        new_worksheet = MagicMock()
        spreadsheet.add_worksheet.return_value = new_worksheet
        config = OutputConfigMother.default()

        result = mock_gspread._get_or_create_worksheet(spreadsheet, config, 10, 5)

        spreadsheet.add_worksheet.assert_called_once_with(
            title=config.tab_name, rows=11, cols=5
        )
        assert result == new_worksheet

    def test_should_update_worksheet(self, mock_gspread):
        worksheet = MagicMock()
        values = [["col1", "col2"], [1, 2]]

        mock_gspread._update_worksheet(worksheet, values)

        worksheet.update.assert_called_once_with(values, value_input_option="RAW")


class TestGSheetExporterCredentials:

    def test_should_use_provided_path(self):
        result = GSheetExporter._resolve_credentials_path("/custom/path.json")
        assert result == "/custom/path.json"

    def test_should_use_env_variable(self):
        with patch.dict("os.environ", {"GOOGLE_APPLICATION_CREDENTIALS": "/env/creds.json"}):
            result = GSheetExporter._resolve_credentials_path(None)
            assert result == "/env/creds.json"

    def test_should_check_default_paths(self):
        with patch.dict("os.environ", {"GOOGLE_APPLICATION_CREDENTIALS": ""}, clear=False):
            with patch("os.path.exists", side_effect=lambda p: p == "credentials.json"):
                result = GSheetExporter._resolve_credentials_path(None)
                assert result == "credentials.json"

    def test_should_return_empty_when_no_credentials_found(self):
        with patch.dict("os.environ", {"GOOGLE_APPLICATION_CREDENTIALS": ""}, clear=False):
            with patch("os.path.exists", return_value=False):
                result = GSheetExporter._resolve_credentials_path(None)
                assert result == ""


class TestGSheetExporterInitialize:

    def test_should_warn_when_no_credentials(self):
        _patch_resolve = (
            "src.context.commissions.infrastructure.exporters"
            ".gsheet_exporter.GSheetExporter._resolve_credentials_path"
        )
        _patch_logger = (
            "src.context.commissions.infrastructure.exporters"
            ".gsheet_exporter.logger"
        )
        with patch(_patch_resolve, return_value=""):
            with patch(_patch_logger) as mock_logger:
                exporter = GSheetExporter.__new__(GSheetExporter)
                exporter.credentials_path = ""
                exporter.client = None
                exporter._initialize_client()

                mock_logger.warning.assert_called()

    def test_should_handle_import_error(self):
        with patch("builtins.__import__", side_effect=ImportError("No module gspread")):
            exporter = GSheetExporter.__new__(GSheetExporter)
            exporter.credentials_path = "/fake/path"
            exporter.client = None
            exporter._initialize_client()

            assert exporter.client is None

    def test_should_handle_generic_initialization_error(self):
        with patch.dict("os.environ", {"GOOGLE_APPLICATION_CREDENTIALS": ""}, clear=False):
            with patch("src.context.commissions.infrastructure.exporters.gsheet_exporter.logger"):
                exporter = GSheetExporter.__new__(GSheetExporter)
                exporter.credentials_path = "/nonexistent/path.json"
                exporter.client = None
                exporter._initialize_client()

    def test_should_initialize_client_with_credentials(self):
        mock_credentials = MagicMock()
        mock_client = MagicMock()

        _patch_resolve = (
            "src.context.commissions.infrastructure.exporters"
            ".gsheet_exporter.GSheetExporter._resolve_credentials_path"
        )
        with patch(_patch_resolve, return_value="/fake/creds.json"):
            with patch.dict("sys.modules", {
                "gspread": MagicMock(authorize=MagicMock(return_value=mock_client)),
                "google.oauth2.service_account": MagicMock(
                    Credentials=MagicMock(
                        from_service_account_file=MagicMock(return_value=mock_credentials)
                    )
                ),
            }):
                exporter = GSheetExporter.__new__(GSheetExporter)
                exporter.credentials_path = "/fake/creds.json"
                exporter.client = None
                exporter._initialize_client()

                assert exporter.client == mock_client

    def test_constructor_calls_resolve_and_initialize(self):
        with patch.object(GSheetExporter, "_resolve_credentials_path", return_value="/fake/path") as mock_resolve:
            with patch.object(GSheetExporter, "_initialize_client") as mock_init:
                exporter = GSheetExporter(credentials_path="/custom/path")

                mock_resolve.assert_called_once_with("/custom/path")
                mock_init.assert_called_once()
                assert exporter.credentials_path == "/fake/path"
