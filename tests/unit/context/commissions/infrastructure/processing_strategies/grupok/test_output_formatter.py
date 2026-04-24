import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupok.output_formatter import (
    GrupoKOutputFormatter,
)


class TestGrupoKOutputFormatter:

    @pytest.fixture
    def formatter(self):
        return GrupoKOutputFormatter()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            'Fecha': ['2025-11-01', '2025-11-01'],
            'Rep ID': ['16766611-6', '18234567-8'],
            'ID Transaccion': ['GK_001', 'GK_002'],
            'Vendedor': ['Pedro', 'Juan'],
            'Sucursal': ['Santiago', 'Valparaiso'],
            'Cantidad Ventas': [2, 1],
            'Monto Neto': [5000000.5, 6000000.7],
            'Comision': [109000.5, 132000.3],
        })


class TestFormat(TestGrupoKOutputFormatter):

    def test_should_return_all_columns_in_order(self, formatter, sample_data):
        result = formatter.format(sample_data)

        expected_order = [
            'Fecha', 'Rep ID', 'ID Transaccion', 'Vendedor',
            'Sucursal', 'Cantidad Ventas', 'Monto Neto', 'Comision'
        ]
        assert list(result.columns) == expected_order

    def test_should_round_money_columns_to_integer(self, formatter, sample_data):
        result = formatter.format(sample_data)

        assert result['Monto Neto'].iloc[0] == 5000000
        assert result['Comision'].iloc[0] == 109000

    def test_should_convert_cantidad_ventas_to_integer(self, formatter, sample_data):
        sample_data['Cantidad Ventas'] = [2.0, 1.0]

        result = formatter.format(sample_data)

        assert result['Cantidad Ventas'].iloc[0] == 2
        assert result['Cantidad Ventas'].iloc[1] == 1

    def test_should_add_column_types_to_attrs(self, formatter, sample_data):
        result = formatter.format(sample_data)

        assert 'column_types' in result.attrs
        assert result.attrs['column_types']['Fecha'] == 'date'
        assert result.attrs['column_types']['Rep ID'] == 'text'
        assert result.attrs['column_types']['Comision'] == 'money'

    def test_should_filter_rows_with_empty_rep_id(self, formatter):
        data = pd.DataFrame({
            'Fecha': ['2025-11-01', '2025-11-01'],
            'Rep ID': ['16766611-6', ''],
            'ID Transaccion': ['GK_001', 'GK_002'],
            'Monto Neto': [3000000, 2000000],
            'Comision': [65400, 43600],
        })

        result = formatter.format(data)

        assert len(result) == 1

    def test_should_sort_by_sucursal_and_rep_id(self, formatter):
        data = pd.DataFrame({
            'Fecha': ['2025-11-01', '2025-11-01'],
            'Rep ID': ['18234567-8', '16766611-6'],
            'ID Transaccion': ['GK_002', 'GK_001'],
            'Sucursal': ['Valparaiso', 'Santiago'],
            'Monto Neto': [3000000, 2000000],
            'Comision': [65400, 43600],
        })

        result = formatter.format(data)

        assert result['Sucursal'].iloc[0] == 'Santiago'
        assert result['Sucursal'].iloc[1] == 'Valparaiso'

    def test_should_handle_empty_dataframe(self, formatter):
        data = pd.DataFrame()

        result = formatter.format(data)

        assert result.empty
