import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client import (
    CopecNewClientCommissionStrategy,
)


class TestCopecNewClientCommissionStrategy:

    @pytest.fixture
    def strategy(self):
        return CopecNewClientCommissionStrategy(
            product_type="TCT",
            discount_percentage=0.10,
            max_factor=2.0,
            bono_nuevo=5000,
            target_period="2025-01-01"
        )

    @pytest.fixture
    def strategy_without_bonus(self):
        return CopecNewClientCommissionStrategy(
            product_type="TCT",
            discount_percentage=0.10,
            max_factor=2.0,
            bono_nuevo=0,
            target_period="2025-01-01"
        )

    @pytest.fixture
    def sample_sales(self):
        return pd.DataFrame({
            'Ejecutivo': ['1234567890', '1234567890', '9876543210'],
            'rut_cliente': ['12345678', '11111111', '22222222'],
            'dv_cliente': ['9', '1', '2'],
            'Producto': ['TCT', 'TCT', 'TCT'],
            'Volumen': [100, 200, 150],
            'Descuento': [5, 10, 8],
            'anio': [2025, 2025, 2025],
            'mes': [1, 1, 1],
        })

    @pytest.fixture
    def sample_historical(self):
        return [
            pd.DataFrame({
                'Ejecutivo': ['1234567890'],
                'rut_cliente': ['99999999'],
                'dv_cliente': ['9'],
                'Producto': ['TCT'],
                'Volumen': [50],
                'Descuento': [3],
                'anio': [2024],
                'mes': [12],
            }),
            pd.DataFrame({
                'Ejecutivo': ['1234567890'],
                'rut_cliente': ['88888888'],
                'dv_cliente': ['8'],
                'Producto': ['TCT'],
                'Volumen': [60],
                'Descuento': [4],
                'anio': [2024],
                'mes': [11],
            }),
        ]


class TestCalculateCommission(TestCopecNewClientCommissionStrategy):

    def test_should_return_empty_dataframe_when_data_is_empty(self, strategy):
        data = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_dataframe_when_no_product_matches(self, strategy):
        data = pd.DataFrame({
            'Ejecutivo': ['1234567890'],
            'rut_cliente': ['12345678'],
            'dv_cliente': ['9'],
            'Producto': ['TAE'],
            'Volumen': [100],
            'Descuento': [5],
            'anio': [2025],
            'mes': [1],
        })
        data.attrs['ventas_historicas'] = []

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_volume_is_zero(self, strategy):
        data = pd.DataFrame({
            'Ejecutivo': ['1234567890'],
            'rut_cliente': ['12345678'],
            'dv_cliente': ['9'],
            'Producto': ['TCT'],
            'Volumen': [0],
            'Descuento': [5],
            'anio': [2025],
            'mes': [1],
        })
        data.attrs['ventas_historicas'] = []

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_set_rep_id_filter_in_strategy(self):
        strategy = CopecNewClientCommissionStrategy(
            product_type="TCT",
            discount_percentage=0.10,
            max_factor=2.0,
            bono_nuevo=5000,
            target_period="2025-01-01",
            rep_id_filter="1234567890"
        )

        assert strategy._rep_id_filter == "1234567890"

    def test_should_have_output_formatter(self, strategy):
        assert strategy._output_formatter is not None


class TestGetColumnTypes(TestCopecNewClientCommissionStrategy):

    def test_should_return_column_types_dict(self, strategy):
        column_types = strategy.get_column_types()

        assert isinstance(column_types, dict)
        assert len(column_types) > 0


class TestNormalizeColumns(TestCopecNewClientCommissionStrategy):

    def test_should_normalize_column_names_to_lowercase(self, strategy):
        data = pd.DataFrame({
            'EJECUTIVO': ['123'],
            'RUT': ['12345678-9'],
            'PRODUCTO': ['TCT'],
        })

        result = strategy._normalize_columns(data)

        assert 'ejecutivo' in result.columns
        assert 'rut' in result.columns
        assert 'producto' in result.columns


class TestFilterByRepId(TestCopecNewClientCommissionStrategy):

    def test_should_filter_dataframe_by_rep_id(self, strategy):
        strategy._rep_id_filter = "1234567890"
        data = pd.DataFrame({
            'ejecutivo': ['1234567890', '9999999999'],
            'rut': ['12345678-9', '11111111-1'],
        })

        result = strategy._filter_by_rep_id(data)

        assert len(result) == 1
        assert result['ejecutivo'].iloc[0] == '1234567890'

    def test_should_pad_rep_id_with_zeros(self, strategy):
        strategy._rep_id_filter = "123"
        data = pd.DataFrame({
            'ejecutivo': ['0000000123', '9999999999'],
            'rut': ['12345678-9', '11111111-1'],
        })

        result = strategy._filter_by_rep_id(data)

        assert len(result) == 1
