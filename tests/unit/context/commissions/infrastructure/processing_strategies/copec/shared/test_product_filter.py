import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.shared import (
    ProductFilter,
)


class TestProductFilter:

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "producto": ["TCT", "TAE", "TCT", "CUPON ELECTRONICO", "tct"],
            "volumen": [100, 200, 0, 300, 50],
            "descuento": [10, 20, 5, 30, 5],
        })

    def test_filter_single_product(self, sample_data):
        pf = ProductFilter("TCT")
        result = pf.filter(sample_data)

        assert len(result) == 3
        assert all(result["producto"].str.upper() == "TCT")

    def test_filter_multiple_products(self, sample_data):
        pf = ProductFilter(["TCT", "TAE"])
        result = pf.filter(sample_data)

        assert len(result) == 4

    def test_filter_case_insensitive(self, sample_data):
        pf = ProductFilter("tct")
        result = pf.filter(sample_data)

        assert len(result) == 3

    def test_filter_raises_error_when_column_missing(self):
        pf = ProductFilter("TCT")
        df = pd.DataFrame({"other": [1, 2, 3]})

        with pytest.raises(ValueError, match="Column 'producto' not found"):
            pf.filter(df)

    def test_filter_positive_volume(self, sample_data):
        pf = ProductFilter("TCT")
        filtered = pf.filter(sample_data)
        result = pf.filter_positive_volume(filtered)

        assert len(result) == 2
        assert all(result["volumen"] > 0)

    def test_filter_positive_volume_raises_error_when_column_missing(self):
        pf = ProductFilter("TCT")
        df = pd.DataFrame({"producto": ["TCT"], "other": [100]})

        with pytest.raises(ValueError, match="Column 'volumen' not found"):
            pf.filter_positive_volume(df)

    def test_custom_volume_column(self):
        df = pd.DataFrame({
            "producto": ["TCT", "TCT"],
            "volumen_lts": [100, 200],
            "descuento": [10, 20],
        })
        pf = ProductFilter("TCT", volume_col="volumen_lts")
        filtered = pf.filter(df)
        result = pf.filter_positive_volume(filtered)

        assert len(result) == 2
        assert "volumen" in result.columns

    def test_custom_discount_column(self):
        df = pd.DataFrame({
            "producto": ["TCT"],
            "volumen": [100],
            "discount": [10],
        })
        pf = ProductFilter("TCT", discount_col="discount")
        filtered = pf.filter(df)
        result = pf.filter_positive_volume(filtered)

        assert "descuento" in result.columns

    def test_returns_empty_when_no_matches(self):
        df = pd.DataFrame({
            "producto": ["TAE", "TAE"],
            "volumen": [100, 200],
            "descuento": [10, 20],
        })
        pf = ProductFilter("TCT")
        result = pf.filter(df)

        assert len(result) == 0
