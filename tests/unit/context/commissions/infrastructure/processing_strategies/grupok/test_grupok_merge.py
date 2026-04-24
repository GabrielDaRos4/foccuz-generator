import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupok.grupok_merge import (
    grupok_sales_advisor_merge,
)


class TestGrupokSalesAdvisorMerge:

    @pytest.fixture
    def employees_df(self):
        return pd.DataFrame([
            {'id': 1, 'rut': '16766611-6', 'full_name': 'Pedro'},
            {'id': 2, 'rut': '17123456-7', 'full_name': 'Maria'},
        ])

    @pytest.fixture
    def sales_df(self):
        return pd.DataFrame([
            {'rut_vendedor': '16766611-6', 'monto_neto': 1000000},
            {'rut_vendedor': '17123456-7', 'monto_neto': 2000000},
        ])

    @pytest.fixture
    def tiers_df(self):
        return pd.DataFrame([
            {'desde': 0, 'hasta': 5000000, 'comision_bruta': 2.18},
            {'desde': 5000001, 'hasta': None, 'comision_bruta': 2.50},
        ])


class TestMerge(TestGrupokSalesAdvisorMerge):

    def test_should_return_sales_dataframe(
        self, employees_df, sales_df, tiers_df
    ):
        dataframes = {
            'employees': employees_df,
            'sales': sales_df,
            'commission_tiers': tiers_df,
        }

        result = grupok_sales_advisor_merge(dataframes)

        assert len(result) == 2
        assert 'rut_vendedor' in result.columns

    def test_should_attach_employees_to_attrs(
        self, employees_df, sales_df, tiers_df
    ):
        dataframes = {
            'employees': employees_df,
            'sales': sales_df,
            'commission_tiers': tiers_df,
        }

        result = grupok_sales_advisor_merge(dataframes)

        assert 'employees' in result.attrs
        assert len(result.attrs['employees']) == 2

    def test_should_attach_tiers_to_attrs(
        self, employees_df, sales_df, tiers_df
    ):
        dataframes = {
            'employees': employees_df,
            'sales': sales_df,
            'commission_tiers': tiers_df,
        }

        result = grupok_sales_advisor_merge(dataframes)

        assert 'commission_tiers' in result.attrs
        assert len(result.attrs['commission_tiers']) == 2

    def test_should_attach_sales_copy_to_attrs(
        self, employees_df, sales_df, tiers_df
    ):
        dataframes = {
            'employees': employees_df,
            'sales': sales_df,
            'commission_tiers': tiers_df,
        }

        result = grupok_sales_advisor_merge(dataframes)

        assert 'sales' in result.attrs
        assert len(result.attrs['sales']) == 2

    def test_should_raise_when_sales_missing(
        self, employees_df, tiers_df
    ):
        dataframes = {
            'employees': employees_df,
            'commission_tiers': tiers_df,
        }

        with pytest.raises(ValueError) as exc_info:
            grupok_sales_advisor_merge(dataframes)

        assert "Sales data not found" in str(exc_info.value)

    def test_should_use_custom_keys_from_config(
        self, employees_df, sales_df, tiers_df
    ):
        dataframes = {
            'emp': employees_df,
            'ventas': sales_df,
            'tramos': tiers_df,
        }
        config = {
            'employees_key': 'emp',
            'sales_key': 'ventas',
            'tiers_key': 'tramos',
        }

        result = grupok_sales_advisor_merge(dataframes, config)

        assert 'employees' in result.attrs
        assert 'commission_tiers' in result.attrs
