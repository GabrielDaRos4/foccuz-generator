from datetime import datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.lubricants import (
    LubricantsOutputFormatter,
)


class TestLubricantsOutputFormatter:

    @pytest.fixture
    def formatter(self):
        return LubricantsOutputFormatter()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "rep_id": ["12345678-9"],
            "solicitante": ["Sol A"],
            "volumen": [100],
            "descuento": [0.05],
            "commission": [5000],
            "commission_per_liter": [50.0],
            "cliente": ["Cliente A"],
            "vendedor": ["Juan Perez"],
        })


class TestFormat(TestLubricantsOutputFormatter):

    def test_renames_columns_to_spanish(self, formatter, sample_data):
        period = datetime(2025, 10, 1)
        result = formatter.format(sample_data, period)

        assert "Rep ID" in result.columns
        assert "Solicitante" in result.columns
        assert "Volumen L" in result.columns
        assert "Comision" in result.columns

    def test_adds_date_column(self, formatter, sample_data):
        period = datetime(2025, 10, 1)
        result = formatter.format(sample_data, period)

        assert "Fecha" in result.columns
        assert result["Fecha"].iloc[0] == "2025-10-01"

    def test_adds_transaction_id(self, formatter, sample_data):
        period = datetime(2025, 10, 1)
        result = formatter.format(sample_data, period)

        assert "ID Transaccion" in result.columns
        assert "2025-10-01" in result["ID Transaccion"].iloc[0]
        assert "12345678-9" in result["ID Transaccion"].iloc[0]

    def test_sets_column_types_in_attrs(self, formatter, sample_data):
        period = datetime(2025, 10, 1)
        result = formatter.format(sample_data, period)

        assert "column_types" in result.attrs
        assert result.attrs["column_types"]["Comision"] == "money"
        assert result.attrs["column_types"]["Fecha"] == "date"

    def test_selects_output_columns(self, formatter, sample_data):
        period = datetime(2025, 10, 1)
        result = formatter.format(sample_data, period)

        assert len(result.columns) <= len(formatter.OUTPUT_COLUMNS)
        assert "cliente" not in result.columns
        assert "vendedor" not in result.columns


class TestCleanData(TestLubricantsOutputFormatter):

    def test_removes_rows_with_empty_rep_id(self, formatter):
        data = pd.DataFrame({
            "rep_id": ["12345678-9", ""],
            "solicitante": ["Sol A", "Sol B"],
            "volumen": [100, 200],
            "commission": [5000, 10000],
            "commission_per_liter": [50.0, 50.0],
        })
        period = datetime(2025, 10, 1)

        result = formatter.format(data, period)

        assert len(result) == 1

    def test_converts_numeric_columns(self, formatter):
        data = pd.DataFrame({
            "rep_id": ["12345678-9"],
            "solicitante": ["Sol A"],
            "volumen": ["100"],
            "descuento": ["0.05"],
            "commission": ["5000"],
            "commission_per_liter": ["50.0"],
        })
        period = datetime(2025, 10, 1)

        result = formatter.format(data, period)

        assert result["Volumen L"].iloc[0] == 100
        assert result["Comision"].iloc[0] == 5000
