import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.lubricants import (
    copec_lubricants_merge,
)


class TestCopecLubricantsMerge:

    @pytest.fixture
    def detail_data(self):
        return pd.DataFrame({
            "Rut": ["12.345.678-9", "98.765.432-1"],
            "Vendedor": ["Juan Perez", "Maria Lopez"],
            "Cliente": ["Cliente A", "Cliente B"],
            "Solicitante": ["Sol A", "Sol B"],
            "Vol. 2025": [100, 200],
            "Comision $": [5000, 10000],
        })

    @pytest.fixture
    def monthly_data(self):
        return pd.DataFrame({
            "Vendedor": ["Juan Perez"],
            "Total": [15000],
        })


class TestMergeWithValidData(TestCopecLubricantsMerge):

    def test_returns_detail_dataframe(self, detail_data, monthly_data):
        dataframes = {
            "detalle_comision_mes": detail_data,
            "comision_venta_mensual": monthly_data,
        }

        result = copec_lubricants_merge(dataframes)

        assert len(result) == 2
        assert "Rut" in result.columns

    def test_attaches_monthly_summary_to_attrs(self, detail_data, monthly_data):
        dataframes = {
            "detalle_comision_mes": detail_data,
            "comision_venta_mensual": monthly_data,
        }

        result = copec_lubricants_merge(dataframes)

        assert "monthly_summary" in result.attrs
        assert len(result.attrs["monthly_summary"]) == 1

    def test_works_without_monthly_data(self, detail_data):
        dataframes = {
            "detalle_comision_mes": detail_data,
        }

        result = copec_lubricants_merge(dataframes)

        assert len(result) == 2
        assert "monthly_summary" not in result.attrs


class TestMergeWithInvalidData(TestCopecLubricantsMerge):

    def test_raises_error_when_detail_missing(self, monthly_data):
        dataframes = {
            "comision_venta_mensual": monthly_data,
        }

        with pytest.raises(ValueError, match="Detail commission data not found"):
            copec_lubricants_merge(dataframes)

    def test_raises_error_when_detail_empty(self, monthly_data):
        dataframes = {
            "detalle_comision_mes": pd.DataFrame(),
            "comision_venta_mensual": monthly_data,
        }

        with pytest.raises(ValueError, match="Detail commission data not found"):
            copec_lubricants_merge(dataframes)
