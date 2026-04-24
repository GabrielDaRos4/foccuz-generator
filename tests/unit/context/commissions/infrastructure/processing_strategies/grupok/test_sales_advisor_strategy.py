import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupok.sales_advisor_strategy import (
    GrupoKSalesAdvisorStrategy,
)


class TestGrupoKSalesAdvisorStrategy:

    @pytest.fixture
    def strategy(self):
        return GrupoKSalesAdvisorStrategy(
            target_period="2025-11-01",
            role_filter="Asesor de venta"
        )

    @pytest.fixture
    def employees_data(self):
        return pd.DataFrame([
            {
                'id': 1001,
                'rut': '16.766.611-6',
                'full_name': 'Pedro Montecinos',
                'current_job': {
                    'role': {
                        'name': 'Asesor de venta'
                    }
                }
            },
            {
                'id': 1002,
                'rut': '17.123.456-7',
                'full_name': 'Maria Garcia',
                'current_job': {
                    'role': {
                        'name': 'Gerente'
                    }
                }
            },
            {
                'id': 1003,
                'rut': '18.234.567-8',
                'full_name': 'Juan Lopez',
                'current_job': {
                    'role': {
                        'name': 'Asesor de venta'
                    }
                }
            }
        ])

    @pytest.fixture
    def sales_data(self):
        return pd.DataFrame([
            {
                'tipo_venta': 'RTL',
                'sucursal': 'Santiago',
                'vendedor': 'Pedro Montecinos',
                'rut_vendedor': '16766611-6',
                'fecha': '2025-11-15',
                'razon_social': 'Cliente A',
                'ndoc': 'F001',
                'monto_neto': 3000000
            },
            {
                'tipo_venta': 'RTL',
                'sucursal': 'Santiago',
                'vendedor': 'Pedro Montecinos',
                'rut_vendedor': '16766611-6',
                'fecha': '2025-11-20',
                'razon_social': 'Cliente B',
                'ndoc': 'F002',
                'monto_neto': 2000000
            },
            {
                'tipo_venta': 'RTL',
                'sucursal': 'Valparaiso',
                'vendedor': 'Juan Lopez',
                'rut_vendedor': '18234567-8',
                'fecha': '2025-11-10',
                'razon_social': 'Cliente C',
                'ndoc': 'F003',
                'monto_neto': 6000000
            },
            {
                'tipo_venta': 'RTL',
                'sucursal': 'Santiago',
                'vendedor': 'Maria Garcia',
                'rut_vendedor': '17123456-7',
                'fecha': '2025-11-12',
                'razon_social': 'Cliente D',
                'ndoc': 'F004',
                'monto_neto': 5000000
            }
        ])

    @pytest.fixture
    def tiers_data(self):
        return pd.DataFrame([
            {'n': 1, 'desde': 0, 'hasta': 5000000, 'comision_bruta': 2.18},
            {'n': 2, 'desde': 5000001, 'hasta': 10000000, 'comision_bruta': 2.20},
            {'n': 3, 'desde': 10000001, 'hasta': 15000000, 'comision_bruta': 2.23},
            {'n': 4, 'desde': 15000001, 'hasta': None, 'comision_bruta': 2.50}
        ])

    def _create_data_with_attrs(
        self,
        employees: pd.DataFrame,
        sales: pd.DataFrame,
        tiers: pd.DataFrame
    ) -> pd.DataFrame:
        df = pd.DataFrame({'_placeholder': [1]})
        df.attrs['employees'] = employees
        df.attrs['sales'] = sales
        df.attrs['commission_tiers'] = tiers
        return df


class TestCalculateCommission(TestGrupoKSalesAdvisorStrategy):

    def test_should_return_dataframe_with_required_columns(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert 'Fecha' in result.columns
        assert 'Rep ID' in result.columns
        assert 'ID Transaccion' in result.columns
        assert 'Comision' in result.columns
        assert 'Cantidad Ventas' in result.columns
        assert 'Monto Neto' in result.columns

    def test_should_filter_employees_by_role(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        unique_reps = result['Rep ID'].unique()
        assert '16766611-6' in unique_reps
        assert '18234567-8' in unique_reps
        assert '17123456-7' not in unique_reps

    def test_should_aggregate_sales_by_seller(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        pedro_row = result[result['Rep ID'] == '16766611-6'].iloc[0]
        assert pedro_row['Cantidad Ventas'] == 2
        assert pedro_row['Monto Neto'] == 5000000

        juan_row = result[result['Rep ID'] == '18234567-8'].iloc[0]
        assert juan_row['Cantidad Ventas'] == 1
        assert juan_row['Monto Neto'] == 6000000

    def test_should_sum_commissions_per_seller(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        pedro_row = result[result['Rep ID'] == '16766611-6'].iloc[0]
        expected_commission = (3000000 * 2.18 / 100) + (2000000 * 2.18 / 100)
        assert pedro_row['Comision'] == int(expected_commission)

    def test_should_return_one_row_per_seller(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert len(result) == 2

    def test_should_return_empty_when_no_employees(
        self, strategy, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(pd.DataFrame(), sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_no_sales(
        self, strategy, employees_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, pd.DataFrame(), tiers_data)

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_no_tiers(
        self, strategy, employees_data, sales_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, pd.DataFrame())

        result = strategy.calculate_commission(data)

        assert result.empty


class TestFilterByRepId(TestGrupoKSalesAdvisorStrategy):

    def test_should_filter_by_rep_id(
        self, employees_data, sales_data, tiers_data
    ):
        strategy = GrupoKSalesAdvisorStrategy(
            target_period="2025-11-01",
            role_filter="Asesor de venta",
            rep_id_filter="16766611-6"
        )
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Rep ID'].iloc[0] == '16766611-6'


class TestPeriodFiltering(TestGrupoKSalesAdvisorStrategy):

    def test_should_filter_by_target_period(
        self, employees_data, tiers_data
    ):
        sales_data = pd.DataFrame([
            {
                'vendedor': 'Pedro Montecinos',
                'rut_vendedor': '16766611-6',
                'fecha': '2025-11-15',
                'ndoc': 'F001',
                'monto_neto': 3000000,
                'sucursal': 'Santiago'
            },
            {
                'vendedor': 'Pedro Montecinos',
                'rut_vendedor': '16766611-6',
                'fecha': '2025-10-15',
                'ndoc': 'F002',
                'monto_neto': 2000000,
                'sucursal': 'Santiago'
            }
        ])
        strategy = GrupoKSalesAdvisorStrategy(
            target_period="2025-11-01",
            role_filter="Asesor de venta"
        )
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Cantidad Ventas'].iloc[0] == 1
        assert result['Monto Neto'].iloc[0] == 3000000


class TestCommissionTiers(TestGrupoKSalesAdvisorStrategy):

    def test_should_get_correct_tier_for_each_range(self, strategy, tiers_data):
        assert strategy._get_commission_rate(1000000, tiers_data) == 2.18
        assert strategy._get_commission_rate(5000000, tiers_data) == 2.18
        assert strategy._get_commission_rate(5000001, tiers_data) == 2.20
        assert strategy._get_commission_rate(10000000, tiers_data) == 2.20
        assert strategy._get_commission_rate(15000001, tiers_data) == 2.50
        assert strategy._get_commission_rate(100000000, tiers_data) == 2.50


class TestRutSanitization(TestGrupoKSalesAdvisorStrategy):

    def test_should_sanitize_rut_with_dots(self, strategy):
        assert strategy._sanitize_rut("16.766.611-6") == "16766611-6"

    def test_should_sanitize_rut_with_spaces(self, strategy):
        assert strategy._sanitize_rut(" 16766611-6 ") == "16766611-6"

    def test_should_uppercase_dv(self, strategy):
        assert strategy._sanitize_rut("16766611-k") == "16766611-K"
