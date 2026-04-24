from datetime import datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    MONTH_NAMES_ES,
    LicensePlateOutputFormatter,
    extract_period,
)
from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    license_plate_output_formatter as _lpof_mod,
)

format_month_name = _lpof_mod.format_month_name


class TestLicensePlateOutputFormatter:

    @pytest.fixture
    def formatter(self):
        return LicensePlateOutputFormatter()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "license_plate_normalized": ["ABC123", "DEF456"],
            "client_rut_complete": ["12345678-9", "98765432-1"],
            "new_client_bonus": [15000, 15000],
            "months_detail": ["1 - Enero", "2 - Febrero"],
            "rep_id": ["1234567890", "9876543210"],
        })


class TestFormat(TestLicensePlateOutputFormatter):

    def test_should_return_dataframe_with_required_columns(
        self, formatter, sample_data
    ):
        period = datetime(2025, 1, 1)

        result = formatter.format(sample_data, period)

        assert "Fecha" in result.columns
        assert "Rep ID" in result.columns
        assert "ID Transaccion" in result.columns
        assert "Rut Cliente" in result.columns
        assert "Patente" in result.columns
        assert "Comision" in result.columns

    def test_should_set_date_from_period(self, formatter, sample_data):
        period = datetime(2025, 3, 15)

        result = formatter.format(sample_data, period)

        assert result["Fecha"].iloc[0] == "2025-03-15"

    def test_should_generate_transaction_id(self, formatter, sample_data):
        period = datetime(2025, 1, 1)

        result = formatter.format(sample_data, period)

        assert "ID Transaccion" in result.columns
        assert "2025-01-01" in result["ID Transaccion"].iloc[0]

    def test_should_calculate_total_commission(self, formatter, sample_data):
        period = datetime(2025, 1, 1)

        result = formatter.format(sample_data, period)

        assert result["Comision"].iloc[0] == 15000

    def test_should_include_column_types_in_attrs(self, formatter, sample_data):
        period = datetime(2025, 1, 1)

        result = formatter.format(sample_data, period)

        assert "column_types" in result.attrs
        assert result.attrs["column_types"]["Comision"] == "money"


class TestPrepareLicensePlate(TestLicensePlateOutputFormatter):

    def test_should_copy_normalized_license_plate(self, formatter):
        df = pd.DataFrame({
            "license_plate_normalized": ["ABC123"],
        })

        result = formatter._prepare_license_plate(df)

        assert result["license_plate"].iloc[0] == "ABC123"


class TestAddDateColumns(TestLicensePlateOutputFormatter):

    def test_should_add_date_column(self, formatter):
        df = pd.DataFrame({
            "license_plate": ["ABC123"],
            "client_rut_complete": ["12345678-9"],
        })
        period = datetime(2025, 6, 15)

        result = formatter._add_date_columns(df, period)

        assert result["date"].iloc[0] == "2025-06-15"

    def test_should_add_transaction_id(self, formatter):
        df = pd.DataFrame({
            "license_plate": ["ABC123"],
            "client_rut_complete": ["12345678-9"],
        })
        period = datetime(2025, 1, 1)

        result = formatter._add_date_columns(df, period)

        assert "transaction_id" in result.columns
        assert "ABC123" in result["transaction_id"].iloc[0]


class TestCalculateTotal(TestLicensePlateOutputFormatter):

    def test_should_set_total_commission_from_bonus(self, formatter):
        df = pd.DataFrame({
            "new_client_bonus": [15000],
        })

        result = formatter._calculate_total(df)

        assert result["total_commission"].iloc[0] == 15000


class TestRenameColumns(TestLicensePlateOutputFormatter):

    def test_should_rename_columns_to_spanish(self, formatter):
        df = pd.DataFrame({
            "client_rut_complete": ["12345678-9"],
            "license_plate": ["ABC123"],
            "rep_id": ["123"],
        })

        result = formatter._rename_columns(df)

        assert "Rut Cliente" in result.columns
        assert "Patente" in result.columns
        assert "Rep ID" in result.columns


class TestSelectOutputColumns(TestLicensePlateOutputFormatter):

    def test_should_select_only_output_columns(self, formatter):
        df = pd.DataFrame({
            "Fecha": ["2025-01-01"],
            "Rep ID": ["123"],
            "extra_column": ["value"],
        })

        result = formatter._select_output_columns(df)

        assert "extra_column" not in result.columns


class TestCleanData(TestLicensePlateOutputFormatter):

    def test_should_convert_numeric_columns(self, formatter):
        df = pd.DataFrame({
            "Bono Cliente Nuevo": ["15000"],
            "Comision": ["15000"],
        })

        result = formatter._clean_data(df)

        assert result["Comision"].iloc[0] == 15000.0

    def test_should_fill_na_with_zero_for_numeric(self, formatter):
        df = pd.DataFrame({
            "Comision": [None],
        })

        result = formatter._clean_data(df)

        assert result["Comision"].iloc[0] == 0


class TestExtractPeriod:

    def test_should_extract_period_from_anio_mes_columns(self):
        df = pd.DataFrame({
            "anio": [2025],
            "mes": [6],
        })

        result = extract_period(df)

        assert result.year == 2025
        assert result.month == 6
        assert result.day == 1

    def test_should_return_current_month_when_columns_missing(self):
        df = pd.DataFrame({
            "other_col": [1],
        })

        result = extract_period(df)

        assert result.day == 1

    def test_should_return_current_month_when_df_empty(self):
        df = pd.DataFrame({
            "anio": [],
            "mes": [],
        })

        result = extract_period(df)

        assert result is not None


class TestFormatMonthName:

    def test_should_format_january(self):
        result = format_month_name(1)

        assert result == "1 - Enero"

    def test_should_format_december(self):
        result = format_month_name(12)

        assert result == "12 - Diciembre"

    def test_should_handle_invalid_month(self):
        result = format_month_name(13)

        assert result == "13 - "


class TestMonthNamesES:

    def test_should_contain_all_12_months(self):
        assert len(MONTH_NAMES_ES) == 12

    def test_should_have_correct_january(self):
        assert MONTH_NAMES_ES[1] == "Enero"

    def test_should_have_correct_december(self):
        assert MONTH_NAMES_ES[12] == "Diciembre"
