import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.shared import (
    copec_new_client_merge,
)


class TestCopecNewClientMerge:

    @pytest.fixture
    def sample_current_sales(self):
        return pd.DataFrame({
            "anio": [2024, 2024],
            "mes": [12, 12],
            "rut_cliente": ["12345678", "87654321"],
            "volumen": [100, 200],
        })

    @pytest.fixture
    def sample_historical_sales(self):
        return pd.DataFrame({
            "anio": [2024, 2024],
            "mes": [11, 11],
            "rut_cliente": ["12345678", "99999999"],
            "volumen": [150, 250],
        })

    def test_returns_current_sales_dataframe(self, sample_current_sales):
        dataframes = {"ventas_mes_actual": sample_current_sales}
        result = copec_new_client_merge(dataframes)

        assert len(result) == 2

    def test_attaches_historical_sales_to_attrs(self, sample_current_sales, sample_historical_sales):
        dataframes = {
            "ventas_mes_actual": sample_current_sales,
            "ventas_mes_1": sample_historical_sales,
        }
        result = copec_new_client_merge(dataframes)

        assert "ventas_historicas" in result.attrs
        assert len(result.attrs["ventas_historicas"]) == 1

    def test_attaches_employees_to_attrs(self, sample_current_sales):
        employees = pd.DataFrame({"id": [1, 2], "name": ["Juan", "Maria"]})
        dataframes = {
            "ventas_mes_actual": sample_current_sales,
            "empleados": employees,
        }
        result = copec_new_client_merge(dataframes)

        assert "empleados" in result.attrs

    def test_raises_error_when_no_sales_data(self):
        with pytest.raises(ValueError, match="No sales data found"):
            copec_new_client_merge({})

    def test_handles_target_period(self, sample_current_sales, sample_historical_sales):
        dataframes = {
            "ventas_mes_actual": sample_current_sales,
            "ventas_mes_1": sample_historical_sales,
        }
        config = {"target_period": "2024-12-01"}
        result = copec_new_client_merge(dataframes, config)

        assert len(result) == 2

    def test_sorts_historical_by_date(self):
        df_oct = pd.DataFrame({"anio": [2024], "mes": [10], "volumen": [100]})
        df_nov = pd.DataFrame({"anio": [2024], "mes": [11], "volumen": [200]})
        df_dec = pd.DataFrame({"anio": [2024], "mes": [12], "volumen": [300]})

        dataframes = {
            "ventas_mes_actual": df_dec,
            "ventas_mes_1": df_oct,
            "ventas_mes_2": df_nov,
        }
        config = {"target_period": "2024-12-01"}
        result = copec_new_client_merge(dataframes, config)

        historical = result.attrs["ventas_historicas"]
        assert len(historical) == 2
