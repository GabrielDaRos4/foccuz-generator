import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupok.product_manager_strategy import (
    GrupoKProductManagerStrategy,
)


class TestGrupoKProductManagerStrategy:

    @pytest.fixture
    def strategy(self):
        return GrupoKProductManagerStrategy(
            target_period="2025-11-01",
            role_code="gerente_de_producto_revestimientos_duros",
            sales_columns=["linea_negocio_rd"]
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
                        'code': 'gerente_de_producto_revestimientos_duros',
                        'name': 'Gerente de Producto RD'
                    }
                }
            },
            {
                'id': 1002,
                'rut': '17.123.456-7',
                'full_name': 'Maria Garcia',
                'current_job': {
                    'role': {
                        'code': 'subgerente_de_producto',
                        'name': 'Subgerente de Producto'
                    }
                }
            }
        ])

    @pytest.fixture
    def sales_data(self):
        return pd.DataFrame([
            {
                'fecha': '2025-11-15',
                'vendedor': 'Seller 1',
                'rut_vendedor': '11111111-1',
                'linea_negocio_rd': 3000000000,
                'linea_negocio_hi': 100000
            },
            {
                'fecha': '2025-11-20',
                'vendedor': 'Seller 2',
                'rut_vendedor': '22222222-2',
                'linea_negocio_rd': 2500000000,
                'linea_negocio_hi': 200000
            },
            {
                'fecha': '2025-10-15',
                'vendedor': 'Seller 3',
                'rut_vendedor': '33333333-3',
                'linea_negocio_rd': 1000000000,
                'linea_negocio_hi': 50000
            }
        ])

    @pytest.fixture
    def tiers_data(self):
        return pd.DataFrame([
            {'n': 1, 'desde': 0, 'hasta': 5400000000, 'comision_bruta': 1850000},
            {'n': 2, 'desde': 5400000001, 'hasta': 5650000000, 'comision_bruta': 1900000},
            {'n': 3, 'desde': 5650000001, 'hasta': 5900000000, 'comision_bruta': 1950000},
            {'n': 4, 'desde': 5900000001, 'hasta': None, 'comision_bruta': 2000000}
        ])

    @staticmethod
    def _create_data_with_attrs(
        employees: pd.DataFrame,
        sales: pd.DataFrame,
        tiers: pd.DataFrame
    ) -> pd.DataFrame:
        df = pd.DataFrame({'_placeholder': [1]})
        df.attrs['employees'] = employees
        df.attrs['sales'] = sales
        df.attrs['commission_tiers'] = tiers
        return df


class TestCalculateCommission(TestGrupoKProductManagerStrategy):

    def test_should_return_dataframe_with_required_columns(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert 'Fecha' in result.columns
        assert 'Rep ID' in result.columns
        assert 'ID Transaccion' in result.columns
        assert 'Comision' in result.columns
        assert 'Total Ventas' in result.columns
        assert 'Product Manager' in result.columns

    def test_should_find_pm_by_role_code(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Product Manager'].iloc[0] == 'Pedro Montecinos'

    def test_should_sum_all_sales_from_sales_columns(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result['Total Ventas'].iloc[0] == 5500000000

    def test_should_filter_by_target_period(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result['Cantidad Ventas'].iloc[0] == 2

    def test_should_get_correct_fixed_commission(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result['Comision'].iloc[0] == 1900000

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

    def test_should_return_empty_when_pm_not_found(
        self, sales_data, tiers_data
    ):
        strategy = GrupoKProductManagerStrategy(
            target_period="2025-11-01",
            role_code="nonexistent_role",
            sales_columns=["linea_negocio_rd"]
        )
        employees = pd.DataFrame([
            {
                'id': 1001,
                'rut': '16.766.611-6',
                'full_name': 'Pedro Montecinos',
                'current_job': {
                    'role': {
                        'code': 'other_role'
                    }
                }
            }
        ])
        data = self._create_data_with_attrs(employees, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result.empty


class TestMultipleSalesColumns(TestGrupoKProductManagerStrategy):

    def test_should_sum_multiple_sales_columns(
        self, employees_data, tiers_data
    ):
        strategy = GrupoKProductManagerStrategy(
            target_period="2025-11-01",
            role_code="gerente_de_producto_revestimientos_duros",
            sales_columns=["linea_negocio_rd", "linea_negocio_hi"]
        )
        sales_data = pd.DataFrame([
            {
                'fecha': '2025-11-15',
                'linea_negocio_rd': 1000000000,
                'linea_negocio_hi': 500000000
            }
        ])
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result['Total Ventas'].iloc[0] == 1500000000


class TestFilterByRepId(TestGrupoKProductManagerStrategy):

    def test_should_filter_by_rep_id(
        self, employees_data, sales_data, tiers_data
    ):
        strategy = GrupoKProductManagerStrategy(
            target_period="2025-11-01",
            role_code="gerente_de_producto_revestimientos_duros",
            sales_columns=["linea_negocio_rd"],
            rep_id_filter="16766611-6"
        )
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Rep ID'].iloc[0] == '16766611-6'

    def test_should_return_empty_when_rep_id_not_found(
        self, employees_data, sales_data, tiers_data
    ):
        strategy = GrupoKProductManagerStrategy(
            target_period="2025-11-01",
            role_code="gerente_de_producto_revestimientos_duros",
            sales_columns=["linea_negocio_rd"],
            rep_id_filter="99999999-9"
        )
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result.empty


class TestCommissionTiers(TestGrupoKProductManagerStrategy):

    def test_should_get_first_tier_for_low_sales(self, tiers_data):
        commission = GrupoKProductManagerStrategy._get_fixed_commission(1000000, tiers_data)

        assert commission == 1850000

    def test_should_get_middle_tier_for_mid_sales(self, tiers_data):
        commission = GrupoKProductManagerStrategy._get_fixed_commission(5600000000, tiers_data)

        assert commission == 1900000

    def test_should_get_last_tier_for_high_sales(self, tiers_data):
        commission = GrupoKProductManagerStrategy._get_fixed_commission(10000000000, tiers_data)

        assert commission == 2000000

    def test_should_return_zero_when_no_matching_tier(self):
        tiers = pd.DataFrame([
            {'desde': 1000000, 'hasta': 2000000, 'comision_bruta': 100}
        ])

        commission = GrupoKProductManagerStrategy._get_fixed_commission(500000, tiers)

        assert commission == 0.0
