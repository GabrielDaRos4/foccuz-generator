import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupok.subgerente_strategy import (
    GrupoKSubgerenteStrategy,
)


class TestGrupoKSubgerenteStrategy:

    @pytest.fixture
    def strategy(self):
        return GrupoKSubgerenteStrategy(
            target_period="2025-11-01",
            pm_role_code="gerente_de_productos_hidrosanitario",
            sales_columns=["linea_negocio_hi"]
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
                        'code': 'gerente_de_productos_hidrosanitario',
                        'name': 'Gerente de Productos Hidrosanitario'
                    },
                    'boss': None
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
                    },
                    'boss': {
                        'id': 1001,
                        'rut': '16.766.611-6',
                        'full_name': 'Pedro Montecinos'
                    }
                }
            },
            {
                'id': 1003,
                'rut': '18.234.567-8',
                'full_name': 'Juan Lopez',
                'current_job': {
                    'role': {
                        'code': 'subgerente_de_producto',
                        'name': 'Subgerente de Producto'
                    },
                    'boss': {
                        'id': 1001,
                        'rut': '16.766.611-6',
                        'full_name': 'Pedro Montecinos'
                    }
                }
            },
            {
                'id': 1004,
                'rut': '19.345.678-9',
                'full_name': 'Ana Torres',
                'current_job': {
                    'role': {
                        'code': 'subgerente_de_producto',
                        'name': 'Subgerente de Producto'
                    },
                    'boss': {
                        'id': 2000,
                        'rut': '20.456.789-0',
                        'full_name': 'Other PM'
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
                'linea_negocio_hi': 1500000000,
                'linea_negocio_rd': 100000
            },
            {
                'fecha': '2025-11-20',
                'vendedor': 'Seller 2',
                'linea_negocio_hi': 800000000,
                'linea_negocio_rd': 200000
            },
            {
                'fecha': '2025-10-15',
                'vendedor': 'Seller 3',
                'linea_negocio_hi': 500000000,
                'linea_negocio_rd': 50000
            }
        ])

    @pytest.fixture
    def tiers_data(self):
        return pd.DataFrame([
            {'n': 1, 'desde': 0, 'hasta': 2000000000, 'comision_bruta': 1900000},
            {'n': 2, 'desde': 2000000001, 'hasta': 2100000000, 'comision_bruta': 2000000},
            {'n': 3, 'desde': 2100000001, 'hasta': 2200000000, 'comision_bruta': 2100000},
            {'n': 4, 'desde': 2200000001, 'hasta': None, 'comision_bruta': 2200000}
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


class TestCalculateCommission(TestGrupoKSubgerenteStrategy):

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
        assert 'Subgerente' in result.columns
        assert 'Product Manager' in result.columns

    def test_should_find_subgerentes_by_boss_rut(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert len(result) == 2
        subgerente_names = result['Subgerente'].tolist()
        assert 'Maria Garcia' in subgerente_names
        assert 'Juan Lopez' in subgerente_names
        assert 'Ana Torres' not in subgerente_names

    def test_should_show_pm_name_in_result(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert all(result['Product Manager'] == 'Pedro Montecinos')

    def test_should_sum_all_sales_from_sales_columns(
        self, strategy, employees_data, sales_data, tiers_data
    ):
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result['Total Ventas'].iloc[0] == 2300000000

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

        assert result['Comision'].iloc[0] == 2200000

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
        strategy = GrupoKSubgerenteStrategy(
            target_period="2025-11-01",
            pm_role_code="nonexistent_role",
            sales_columns=["linea_negocio_hi"]
        )
        employees = pd.DataFrame([
            {
                'id': 1001,
                'rut': '16.766.611-6',
                'full_name': 'Pedro Montecinos',
                'current_job': {
                    'role': {
                        'code': 'other_role'
                    },
                    'boss': None
                }
            }
        ])
        data = self._create_data_with_attrs(employees, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_no_subgerentes_for_pm(
        self, sales_data, tiers_data
    ):
        strategy = GrupoKSubgerenteStrategy(
            target_period="2025-11-01",
            pm_role_code="gerente_de_productos_hidrosanitario",
            sales_columns=["linea_negocio_hi"]
        )
        employees = pd.DataFrame([
            {
                'id': 1001,
                'rut': '16.766.611-6',
                'full_name': 'Pedro Montecinos',
                'current_job': {
                    'role': {
                        'code': 'gerente_de_productos_hidrosanitario'
                    },
                    'boss': None
                }
            },
            {
                'id': 1002,
                'rut': '17.123.456-7',
                'full_name': 'Maria Garcia',
                'current_job': {
                    'role': {
                        'code': 'subgerente_de_producto'
                    },
                    'boss': {
                        'rut': '99.999.999-9',
                        'full_name': 'Other PM'
                    }
                }
            }
        ])
        data = self._create_data_with_attrs(employees, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result.empty


class TestFilterByRepId(TestGrupoKSubgerenteStrategy):

    def test_should_filter_by_rep_id(
        self, employees_data, sales_data, tiers_data
    ):
        strategy = GrupoKSubgerenteStrategy(
            target_period="2025-11-01",
            pm_role_code="gerente_de_productos_hidrosanitario",
            sales_columns=["linea_negocio_hi"],
            rep_id_filter="17123456-7"
        )
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Subgerente'].iloc[0] == 'Maria Garcia'

    def test_should_return_empty_when_rep_id_not_found(
        self, employees_data, sales_data, tiers_data
    ):
        strategy = GrupoKSubgerenteStrategy(
            target_period="2025-11-01",
            pm_role_code="gerente_de_productos_hidrosanitario",
            sales_columns=["linea_negocio_hi"],
            rep_id_filter="99999999-9"
        )
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result.empty


class TestMultipleSalesColumns(TestGrupoKSubgerenteStrategy):

    def test_should_sum_multiple_sales_columns(
        self, employees_data, tiers_data
    ):
        strategy = GrupoKSubgerenteStrategy(
            target_period="2025-11-01",
            pm_role_code="gerente_de_productos_hidrosanitario",
            sales_columns=["linea_negocio_hi", "linea_negocio_rd"]
        )
        sales_data = pd.DataFrame([
            {
                'fecha': '2025-11-15',
                'linea_negocio_hi': 1000000000,
                'linea_negocio_rd': 500000000
            }
        ])
        data = self._create_data_with_attrs(employees_data, sales_data, tiers_data)

        result = strategy.calculate_commission(data)

        assert result['Total Ventas'].iloc[0] == 1500000000
