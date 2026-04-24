import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.gocar import (
    gocar_commission_merge,
)


class TestGocarCommissionMerge:

    @pytest.fixture
    def sales_dataframe(self):
        return pd.DataFrame({
            "AGENTE O VENDEDOR": ["KARLA RUIZ", "AIMME AGUILAR"],
            "CLIENTE": ["Cliente A", "Cliente B"],
            "COMISION": [100.0, 200.0],
        })

    @pytest.fixture
    def users_dataframe(self):
        return pd.DataFrame({
            "Rep ID": [170865, 170862],
            "Nombre": ["KARLA RUIZ", "AIMME AGUILAR"],
        })


class TestMergeWithValidData(TestGocarCommissionMerge):

    def test_should_return_sales_data_with_users_mapping(self, sales_dataframe, users_dataframe):
        dataframes = {
            "sales": sales_dataframe,
            "users_mapping": users_dataframe,
        }

        result = gocar_commission_merge(dataframes)

        assert len(result) == 2
        assert "users_mapping" in result.attrs
        assert len(result.attrs["users_mapping"]) == 2

    def test_should_use_custom_keys_from_config(self, sales_dataframe, users_dataframe):
        dataframes = {
            "custom_sales": sales_dataframe,
            "custom_users": users_dataframe,
        }
        config = {
            "sales_key": "custom_sales",
            "users_key": "custom_users",
        }

        result = gocar_commission_merge(dataframes, config)

        assert len(result) == 2
        assert "users_mapping" in result.attrs


class TestMergeWithMissingData(TestGocarCommissionMerge):

    def test_should_raise_error_when_sales_not_found(self, users_dataframe):
        dataframes = {"users_mapping": users_dataframe}

        with pytest.raises(ValueError, match="Sales data not found"):
            gocar_commission_merge(dataframes)

    def test_should_raise_error_when_sales_is_empty(self, users_dataframe):
        dataframes = {
            "sales": pd.DataFrame(),
            "users_mapping": users_dataframe,
        }

        with pytest.raises(ValueError, match="Sales data not found"):
            gocar_commission_merge(dataframes)

    def test_should_work_without_users_mapping(self, sales_dataframe):
        dataframes = {"sales": sales_dataframe}

        result = gocar_commission_merge(dataframes)

        assert len(result) == 2
        assert "users_mapping" not in result.attrs or result.attrs.get("users_mapping") is None
