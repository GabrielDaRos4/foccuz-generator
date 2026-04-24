import numpy as np
import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.gocar import (
    GocarOutputFormatter,
)


class TestGocarOutputFormatter:

    @pytest.fixture
    def formatter(self):
        return GocarOutputFormatter()

    @pytest.fixture
    def sample_dataframe(self):
        return pd.DataFrame({
            "Fecha": ["2025-11-01", "2025-11-01"],
            "Rep ID": [170865.0, 170862.0],
            "ID Transaccion": ["NUEVOS_170865_ABC123_1", "NUEVOS_170862_DEF456_2"],
            "Negocio": ["NUEVOS UNIVERSIDAD", "NUEVOS SAN ANGEL"],
            "Cliente": ["Cliente A", "Cliente B"],
            "Chasis": ["ABC123", "DEF456"],
            "Condiciones": ["Contado", "Credito"],
            "Factura": ["F001", "F002"],
            "Utilidad Bruta": [18419.29, 24216.05],
            "% Comision": [0.14, 0.18],
            "Comision Base": [2578.70, 4358.89],
            "Toma": [100.0, 200.0],
            "Financiamiento": [50.0, 75.0],
            "Edegas": [25.0, 30.0],
            "Verificacion": [10.0, 15.0],
            "Accesorios": [20.0, 25.0],
            "Garantias": [5.0, 10.0],
            "Seguros": [15.0, 20.0],
            "Placas": [8.0, 12.0],
            "Bonos Otros": [50.0, 100.0],
            "Semana": [46, 46],
            "Comision": [2861.70, 4845.89],
        })


class TestFormat(TestGocarOutputFormatter):

    def test_should_return_empty_dataframe_when_input_is_empty(self, formatter):
        result = formatter.format(pd.DataFrame())

        assert result.empty

    def test_should_select_only_output_columns(self, formatter, sample_dataframe):
        sample_dataframe["ExtraColumn"] = ["extra1", "extra2"]

        result = formatter.format(sample_dataframe)

        assert "ExtraColumn" not in result.columns
        assert "Fecha" in result.columns
        assert "Rep ID" in result.columns

    def test_should_convert_rep_id_to_string(self, formatter, sample_dataframe):
        result = formatter.format(sample_dataframe)

        assert result["Rep ID"].iloc[0] == "170865"
        assert result["Rep ID"].iloc[1] == "170862"

    def test_should_round_money_columns_to_two_decimals(self, formatter):
        df = pd.DataFrame({
            "Rep ID": [170865.0],
            "Utilidad Bruta": [18419.29567],
            "Comision Base": [2578.70123],
            "Comision": [2861.70999],
        })

        result = formatter.format(df)

        assert result["Utilidad Bruta"].iloc[0] == pytest.approx(18419.30, rel=0.01)
        assert result["Comision Base"].iloc[0] == pytest.approx(2578.70, rel=0.01)
        assert result["Comision"].iloc[0] == pytest.approx(2861.71, rel=0.01)

    def test_should_convert_semana_to_integer(self, formatter):
        df = pd.DataFrame({
            "Rep ID": [170865.0],
            "Semana": [46.5],
        })

        result = formatter.format(df)

        assert result["Semana"].iloc[0] == 46
        assert np.issubdtype(type(result["Semana"].iloc[0]), np.integer)

    def test_should_set_column_types_attribute(self, formatter, sample_dataframe):
        result = formatter.format(sample_dataframe)

        assert "column_types" in result.attrs
        assert result.attrs["column_types"]["Fecha"] == "date"
        assert result.attrs["column_types"]["Rep ID"] == "text"
        assert result.attrs["column_types"]["Comision"] == "money"

    def test_should_filter_out_rows_without_rep_id(self, formatter):
        df = pd.DataFrame({
            "Rep ID": [170865.0, None, 170862.0],
            "Comision": [100.0, 200.0, 300.0],
        })

        result = formatter.format(df)

        assert len(result) == 2


class TestColumnTypes(TestGocarOutputFormatter):

    def test_should_have_all_expected_column_types(self, formatter):
        expected_columns = [
            "Fecha", "Rep ID", "ID Transaccion", "Negocio", "Cliente",
            "Chasis", "Condiciones", "Factura", "Utilidad Bruta", "% Comision",
            "Comision Base", "Toma", "Financiamiento", "Edegas", "Verificacion",
            "Accesorios", "Garantias", "Seguros", "Placas", "Bonos Otros",
            "Semana", "Comision",
        ]

        for col in expected_columns:
            assert col in formatter.COLUMN_TYPES
