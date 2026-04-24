import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
    Brand,
    BrandClassifier,
    SalesFilter,
)


class TestSalesFilter:

    @pytest.fixture
    def sales_filter(self):
        return SalesFilter(BrandClassifier())

    @pytest.fixture
    def sample_sales(self):
        return pd.DataFrame({
            "Model": ["CR-V", "RDX", "CIVIC", "MDX", "HR-V"],
            "Brand": ["HONDA", "ACURA", "HONDA", "ACURA", "HONDA"],
            "Status": ["Entregado", "Entregado", "Facturado", "Entregado", "Entregado"],
            "Delivery_Date": [
                "2024-12-01", "2024-12-15", "2024-12-10",
                "2024-11-20", "2024-12-25"
            ],
            "IdConsultant": [1, 2, 1, 3, 2]
        })


class TestFilterByBrand(TestSalesFilter):

    def test_filters_honda_models(self, sales_filter, sample_sales):
        result = sales_filter.filter_by_brand(sample_sales, Brand.HONDA)

        assert len(result) == 3
        models = result["Model"].tolist()
        assert "CR-V" in models
        assert "CIVIC" in models
        assert "HR-V" in models

    def test_filters_acura_models(self, sales_filter, sample_sales):
        result = sales_filter.filter_by_brand(sample_sales, Brand.ACURA)

        assert len(result) == 2
        models = result["Model"].tolist()
        assert "RDX" in models
        assert "MDX" in models

    def test_returns_empty_when_no_matches(self, sales_filter):
        df = pd.DataFrame({
            "Model": ["TESLA", "BMW"],
            "Brand": ["OTHER", "OTHER"],
            "Status": ["Entregado", "Entregado"]
        })

        result = sales_filter.filter_by_brand(df, Brand.HONDA)
        assert len(result) == 0

    def test_raises_error_when_no_brand_column(self, sales_filter):
        df = pd.DataFrame({"Other": [1, 2, 3]})

        with pytest.raises(ValueError, match="No 'brand' column found"):
            sales_filter.filter_by_brand(df, Brand.HONDA)


class TestFilterDelivered(TestSalesFilter):

    def test_filters_delivered_sales(self, sales_filter, sample_sales):
        result = sales_filter.filter_delivered(sample_sales)

        assert len(result) == 4
        assert all(result["Status"].str.lower() == "entregado")

    def test_handles_case_insensitive_status(self, sales_filter):
        df = pd.DataFrame({
            "Status": ["ENTREGADO", "entregado", "Entregado", "Facturado"]
        })

        result = sales_filter.filter_delivered(df)
        assert len(result) == 3

    def test_returns_all_when_no_status_column(self, sales_filter):
        df = pd.DataFrame({"Other": [1, 2, 3]})

        result = sales_filter.filter_delivered(df)
        assert len(result) == 3


class TestFilterByPeriod(TestSalesFilter):

    def test_filters_by_year_and_month(self, sales_filter, sample_sales):
        result = sales_filter.filter_by_period(sample_sales, 2024, 12)

        assert len(result) == 4

    def test_excludes_different_month(self, sales_filter, sample_sales):
        result = sales_filter.filter_by_period(sample_sales, 2024, 11)

        assert len(result) == 1

    def test_excludes_different_year(self, sales_filter, sample_sales):
        result = sales_filter.filter_by_period(sample_sales, 2023, 12)

        assert len(result) == 0

    def test_returns_all_when_no_date_column(self, sales_filter):
        df = pd.DataFrame({"Other": [1, 2, 3]})

        result = sales_filter.filter_by_period(df, 2024, 12)
        assert len(result) == 3

    def test_handles_invalid_dates(self, sales_filter):
        df = pd.DataFrame({
            "Delivery_Date": ["2024-12-01", "invalid", "2024-12-15"]
        })

        result = sales_filter.filter_by_period(df, 2024, 12)
        assert len(result) == 2
