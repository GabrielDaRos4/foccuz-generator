from datetime import datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client import (
    CopecOutputFormatter,
    extract_period,
)


class TestCopecOutputFormatter:

    @pytest.fixture
    def formatter(self):
        return CopecOutputFormatter()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "rep_id": ["12345678-9"],
            "client_rut_complete": ["87654321-K"],
            "volumen": [1000],
            "descuento": [100],
            "unit_commission": [5.0],
            "commission_amount": [5000],
            "new_client_bonus": [10000],
            "total_commission": [15000],
            "client_type": ["SI"],
            "nombre_cliente": ["Test Client"],
        })


class TestFormat(TestCopecOutputFormatter):

    def test_renames_columns(self, formatter, sample_data):
        period = datetime(2024, 12, 1)
        result = formatter.format(sample_data, period)

        assert "Rep ID" in result.columns
        assert "Rut Cliente" in result.columns
        assert "Volumen L" in result.columns

    def test_adds_date_column(self, formatter, sample_data):
        period = datetime(2024, 12, 1)
        result = formatter.format(sample_data, period)

        assert "Fecha" in result.columns
        assert result["Fecha"].iloc[0] == "2024-12-01"

    def test_adds_transaction_id(self, formatter, sample_data):
        period = datetime(2024, 12, 1)
        result = formatter.format(sample_data, period)

        assert "ID Transaccion" in result.columns
        assert "2024-12-01" in result["ID Transaccion"].iloc[0]

    def test_sets_column_types_in_attrs(self, formatter, sample_data):
        period = datetime(2024, 12, 1)
        result = formatter.format(sample_data, period)

        assert "column_types" in result.attrs
        assert result.attrs["column_types"]["Comision"] == "money"

    def test_selects_output_columns(self, formatter, sample_data):
        period = datetime(2024, 12, 1)
        result = formatter.format(sample_data, period)

        assert len(result.columns) <= len(formatter.output_columns)


class TestExtractPeriod:

    def test_extracts_period_from_dataframe(self):
        df = pd.DataFrame({
            "anio": [2024],
            "mes": [12],
        })
        result = extract_period(df)

        assert result.year == 2024
        assert result.month == 12
        assert result.day == 1

    def test_returns_current_date_when_columns_missing(self):
        df = pd.DataFrame({"other": [1]})
        result = extract_period(df)

        assert result.day == 1

    def test_returns_current_date_when_empty(self):
        df = pd.DataFrame({"anio": [], "mes": []})
        result = extract_period(df)

        assert result.day == 1
