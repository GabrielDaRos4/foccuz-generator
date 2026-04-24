import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
    monedero_sales_merge,
)


class TestMerge:

    def test_should_return_sales_dataframe(self):
        sales_df = pd.DataFrame({
            "Id": ["T001", "T002"],
            "Brand": ["HONDA", "ACURA"]
        })

        result = monedero_sales_merge(
            dataframes={"sales": sales_df},
            config={}
        )

        assert len(result) == 2
        assert "Id" in result.columns

    def test_should_raise_when_sales_missing(self):
        with pytest.raises(ValueError, match="Sales dataframe is required"):
            monedero_sales_merge(dataframes={}, config={})

    def test_should_return_copy_of_sales(self):
        sales_df = pd.DataFrame({
            "Id": ["T001"],
            "Brand": ["HONDA"]
        })

        result = monedero_sales_merge(
            dataframes={"sales": sales_df},
            config={}
        )

        result["Id"] = "MODIFIED"
        assert sales_df["Id"].iloc[0] == "T001"
