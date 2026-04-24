import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.shared import (
    RutBuilder,
)


class TestRutBuilder:

    @pytest.fixture
    def builder(self):
        return RutBuilder()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "rut_cliente": ["12345678", "87654321"],
            "dv_cliente": ["9", "K"],
            "rut_ejecutivo": ["11111111", "22222222"],
            "dv_ejecutivo": ["1", "2"],
            "ejecutivo": [1000, 2000],
        })


class TestBuild(TestRutBuilder):

    def test_builds_client_rut_complete(self, builder, sample_data):
        result = builder.build(sample_data)

        assert "client_rut_complete" in result.columns
        assert result["client_rut_complete"].iloc[0] == "12345678-9"
        assert result["client_rut_complete"].iloc[1] == "87654321-K"

    def test_builds_executive_rut_complete(self, builder, sample_data):
        result = builder.build(sample_data)

        assert "executive_rut_complete" in result.columns
        assert result["executive_rut_complete"].iloc[0] == "11111111-1"

    def test_builds_rep_id(self, builder, sample_data):
        result = builder.build(sample_data)

        assert "rep_id" in result.columns
        assert result["rep_id"].iloc[0] == "11111111-1"

    def test_handles_rut_already_with_dv(self, builder):
        df = pd.DataFrame({
            "rut_cliente": ["12345678-9"],
            "dv_cliente": ["9"],
            "rut_ejecutivo": ["11111111-1"],
            "dv_ejecutivo": ["1"],
            "ejecutivo": [1000],
        })
        result = builder.build(df)

        assert result["client_rut_complete"].iloc[0] == "12345678-9"

    def test_raises_error_when_client_columns_missing(self, builder):
        df = pd.DataFrame({"other": [1, 2, 3]})

        with pytest.raises(ValueError, match="Columns 'rut_cliente' and 'dv_cliente' required"):
            builder.build(df)

    def test_handles_sin_informacion_exec_1000491(self, builder):
        df = pd.DataFrame({
            "rut_cliente": ["12345678"],
            "dv_cliente": ["9"],
            "rut_ejecutivo": ["SIN_INFORMACION"],
            "dv_ejecutivo": ["X"],
            "ejecutivo": [1000491],
        })
        result = builder.build(df)

        assert result["rep_id"].iloc[0] == "13573543-4"

    def test_handles_sin_informacion_exec_1000627(self, builder):
        df = pd.DataFrame({
            "rut_cliente": ["12345678"],
            "dv_cliente": ["9"],
            "rut_ejecutivo": ["SIN_INFORMACION"],
            "dv_ejecutivo": ["X"],
            "ejecutivo": [1000627],
        })
        result = builder.build(df)

        assert result["rep_id"].iloc[0] == "11796672-0"


class TestExtractClientRuts(TestRutBuilder):

    def test_extracts_ruts_for_product(self, builder):
        df = pd.DataFrame({
            "producto": ["TCT", "TCT", "TAE"],
            "rut_cliente": ["12345678", "87654321", "11111111"],
            "dv_cliente": ["9", "K", "1"],
            "volumen": [100, 200, 300],
        })
        result = builder.extract_client_ruts(df, "TCT")

        assert len(result) == 2
        assert "12345678-9" in result
        assert "87654321-K" in result

    def test_extracts_ruts_for_multiple_products(self, builder):
        df = pd.DataFrame({
            "producto": ["TCT", "TAE", "CUPON"],
            "rut_cliente": ["12345678", "87654321", "11111111"],
            "dv_cliente": ["9", "K", "1"],
            "volumen": [100, 200, 300],
        })
        result = builder.extract_client_ruts(df, ["TCT", "TAE"])

        assert len(result) == 2

    def test_filters_zero_volume(self, builder):
        df = pd.DataFrame({
            "producto": ["TCT", "TCT"],
            "rut_cliente": ["12345678", "87654321"],
            "dv_cliente": ["9", "K"],
            "volumen": [100, 0],
        })
        result = builder.extract_client_ruts(df, "TCT")

        assert len(result) == 1
        assert "12345678-9" in result

    def test_returns_empty_when_no_producto_column(self, builder):
        df = pd.DataFrame({"other": [1, 2, 3]})
        result = builder.extract_client_ruts(df, "TCT")

        assert len(result) == 0

    def test_returns_empty_when_no_rut_columns(self, builder):
        df = pd.DataFrame({
            "producto": ["TCT"],
            "volumen": [100],
        })
        result = builder.extract_client_ruts(df, "TCT")

        assert len(result) == 0
