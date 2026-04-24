from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.poa_compliance.output_formatter import (
    METRIC_TYPE_MARGEN,
    METRIC_TYPE_VOLUMEN,
    PoaComplianceOutputFormatter,
)


class TestPoaComplianceOutputFormatterVolumen:

    @pytest.fixture
    def formatter(self):
        return PoaComplianceOutputFormatter(metric_type=METRIC_TYPE_VOLUMEN)

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "rep_id": ["12345678-9", "12345678-9", "87654321-K"],
            "producto": ["TCT", "TAE", "TCT"],
            "producto_label": ["TCT (M3)", "TAE (M3)", "TCT (M3)"],
            "valor": [1000, 2000, 500],
            "volume_unit": ["M3", "M3", "M3"],
        })

    @pytest.fixture
    def period(self):
        return datetime(2025, 10, 1)


class TestFormatVolumen(TestPoaComplianceOutputFormatterVolumen):

    def test_should_add_date_column_with_formatted_period(
        self, formatter, sample_data, period
    ):
        result = formatter.format(sample_data, period)

        assert "Fecha" in result.columns
        assert result["Fecha"].iloc[0] == "2025-10-01"

    def test_should_generate_transaction_id_with_product(self, formatter, sample_data, period):
        result = formatter.format(sample_data, period)

        assert "ID Transaccion" in result.columns
        tct_row = result[result["Producto"] == "TCT (M3)"].iloc[0]
        assert "2025-10-01" in tct_row["ID Transaccion"]
        assert "TCT" in tct_row["ID Transaccion"]

    def test_should_use_producto_column_name(self, formatter, sample_data, period):
        result = formatter.format(sample_data, period)

        assert "Producto" in result.columns
        assert result["Producto"].iloc[0] == "TCT (M3)"

    def test_should_create_value_column_with_month_year(self, formatter, sample_data, period):
        result = formatter.format(sample_data, period)

        assert "Real Oct 2025" in result.columns
        assert result["Real Oct 2025"].iloc[0] == 1000

    def test_should_add_comision_column_with_zero(self, formatter, sample_data, period):
        result = formatter.format(sample_data, period)

        assert "Comision" in result.columns
        assert all(result["Comision"] == 0)

    def test_should_convert_value_to_integer_for_volumen(self, formatter, period):
        data = pd.DataFrame({
            "rep_id": ["12345678-9"],
            "producto": ["TCT"],
            "producto_label": ["TCT (M3)"],
            "valor": [1000.75],
            "volume_unit": ["M3"],
        })

        result = formatter.format(data, period)

        assert result["Real Oct 2025"].iloc[0] == 1000
        assert np.issubdtype(result["Real Oct 2025"].dtype, np.integer)

    def test_should_add_column_types_to_attrs(self, formatter, sample_data, period):
        result = formatter.format(sample_data, period)

        assert "column_types" in result.attrs
        assert result.attrs["column_types"]["Fecha"] == "date"
        assert result.attrs["column_types"]["Rep ID"] == "text"
        assert result.attrs["column_types"]["Producto"] == "text"
        assert result.attrs["column_types"]["Real Oct 2025"] == "integer"
        assert result.attrs["column_types"]["Comision"] == "money"


class TestPoaComplianceOutputFormatterMargen:

    @pytest.fixture
    def formatter(self):
        return PoaComplianceOutputFormatter(metric_type=METRIC_TYPE_MARGEN)

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "rep_id": ["12345678-9", "12345678-9"],
            "producto": ["TCT", "TAE"],
            "producto_label": ["TCT ($/M3)", "TAE ($/M3)"],
            "valor": [50.123, 40.567],
            "volume_unit": ["M3", "M3"],
        })

    @pytest.fixture
    def period(self):
        return datetime(2025, 10, 1)


class TestFormatMargen(TestPoaComplianceOutputFormatterMargen):

    def test_should_use_producto_column_name_for_margen(self, formatter, sample_data, period):
        result = formatter.format(sample_data, period)

        assert "Producto" in result.columns
        assert result["Producto"].iloc[0] == "TCT ($/M3)"

    def test_should_round_margin_value_to_two_decimals(self, formatter, sample_data, period):
        result = formatter.format(sample_data, period)

        assert result["Real Oct 2025"].iloc[0] == 50.12

    def test_should_add_column_types_for_margen(self, formatter, sample_data, period):
        result = formatter.format(sample_data, period)

        assert result.attrs["column_types"]["Producto"] == "text"
        assert result.attrs["column_types"]["Real Oct 2025"] == "number"


class TestProductOrdering(TestPoaComplianceOutputFormatterVolumen):

    def test_should_order_products_within_rep(self, formatter, period):
        data = pd.DataFrame({
            "rep_id": ["12345678-9", "12345678-9", "12345678-9"],
            "producto": ["Lubricantes", "TCT", "TAE"],
            "producto_label": ["Lubricantes (L)", "TCT (M3)", "TAE (M3)"],
            "valor": [100, 200, 300],
            "volume_unit": ["L", "M3", "M3"],
        })

        result = formatter.format(data, period)

        rep_rows = result[result["Rep ID"] == "12345678-9"]
        products = rep_rows["Producto"].tolist()
        assert products == ["TCT (M3)", "TAE (M3)", "Lubricantes (L)"]


class TestDataCleaning(TestPoaComplianceOutputFormatterVolumen):

    def test_should_filter_rows_with_empty_rep_id(self, formatter, period):
        data = pd.DataFrame({
            "rep_id": ["12345678-9", "", "87654321-K"],
            "producto": ["TCT", "TCT", "TCT"],
            "producto_label": ["TCT (M3)", "TCT (M3)", "TCT (M3)"],
            "valor": [1000, 500, 800],
            "volume_unit": ["M3", "M3", "M3"],
        })

        result = formatter.format(data, period)

        assert len(result) == 2

    def test_should_handle_non_numeric_value(self, formatter, period):
        data = pd.DataFrame({
            "rep_id": ["12345678-9"],
            "producto": ["TCT"],
            "producto_label": ["TCT (M3)"],
            "valor": ["invalid"],
            "volume_unit": ["M3"],
        })

        result = formatter.format(data, period)

        assert result["Real Oct 2025"].iloc[0] == 0

    def test_should_handle_empty_dataframe(self, formatter, period):
        data = pd.DataFrame(columns=["rep_id", "producto", "producto_label", "valor", "volume_unit"])

        result = formatter.format(data, period)

        assert len(result) == 0


class TestSpanishMonths:

    def test_should_use_spanish_month_names(self):
        formatter = PoaComplianceOutputFormatter(metric_type=METRIC_TYPE_VOLUMEN)
        data = pd.DataFrame({
            "rep_id": ["12345678-9"],
            "producto": ["TCT"],
            "producto_label": ["TCT (M3)"],
            "valor": [1000],
            "volume_unit": ["M3"],
        })

        periods_and_expected = [
            (datetime(2025, 1, 1), "Real Ene 2025"),
            (datetime(2025, 6, 1), "Real Jun 2025"),
            (datetime(2025, 12, 1), "Real Dic 2025"),
        ]

        for period, expected_col in periods_and_expected:
            result = formatter.format(data.copy(), period)
            assert expected_col in result.columns, f"Expected {expected_col} for {period}"
