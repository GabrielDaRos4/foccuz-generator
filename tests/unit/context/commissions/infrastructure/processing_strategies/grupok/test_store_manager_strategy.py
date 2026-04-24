import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupok.store_manager_strategy import (
    GrupoKStoreManagerStrategy,
)
from tests.mothers.commissions.infrastructure import GrupoKDataFrameMother


class TestGrupoKStoreManagerStrategy:

    @pytest.fixture
    def strategy(self):
        return GrupoKStoreManagerStrategy(
            target_period="2025-11-01",
            role_filter="Jefe de Tienda"
        )

    @pytest.fixture
    def mother(self):
        return GrupoKDataFrameMother


class TestCalculateCommission(TestGrupoKStoreManagerStrategy):

    def test_should_return_fecha_column_when_calculating_commission(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert 'Fecha' in result.columns

    def test_should_return_rep_id_column_when_calculating_commission(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert 'Rep ID' in result.columns

    def test_should_return_jefe_de_tienda_column_when_calculating_commission(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert 'Jefe de Tienda' in result.columns

    def test_should_return_comision_column_when_calculating_commission(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert 'Comision' in result.columns

    def test_should_include_jefe_de_tienda_employees_when_filtering_by_role(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert '13215149-0' in result['Rep ID'].values

    def test_should_exclude_non_jefe_employees_when_filtering_by_role(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert '18234567-8' not in result['Rep ID'].values

    def test_should_sum_iquique_sales_when_calculating_total_per_store(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        iquique_row = result[result['Sucursal'] == 'Iquique'].iloc[0]
        assert iquique_row['Total Ventas'] == 45000000

    def test_should_sum_antofagasta_sales_when_calculating_total_per_store(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        antofagasta_row = result[result['Sucursal'] == 'Antofagasta'].iloc[0]
        assert antofagasta_row['Total Ventas'] == 60000000

    def test_should_apply_820000_commission_when_sales_between_40m_and_56m(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        iquique_row = result[result['Sucursal'] == 'Iquique'].iloc[0]
        assert iquique_row['Comision'] == 820000

    def test_should_apply_940000_commission_when_sales_between_56m_and_72m(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        antofagasta_row = result[result['Sucursal'] == 'Antofagasta'].iloc[0]
        assert antofagasta_row['Comision'] == 940000

    def test_should_return_empty_dataframe_when_no_employees(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.empty(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_dataframe_when_no_sales(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.empty(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_dataframe_when_no_tiers(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.empty()
        )

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_count_two_sales_when_iquique_has_two_transactions(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        iquique_row = result[result['Sucursal'] == 'Iquique'].iloc[0]
        assert iquique_row['Cantidad Ventas'] == 2

    def test_should_count_one_sale_when_antofagasta_has_one_transaction(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        antofagasta_row = result[result['Sucursal'] == 'Antofagasta'].iloc[0]
        assert antofagasta_row['Cantidad Ventas'] == 1


class TestFilterByRepId(TestGrupoKStoreManagerStrategy):

    def test_should_return_single_record_when_filtering_by_specific_rep_id(
        self, mother
    ):
        strategy = GrupoKStoreManagerStrategy(
            target_period="2025-11-01",
            role_filter="Jefe de Tienda",
            rep_id_filter="13215149-0"
        )
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert len(result) == 1

    def test_should_return_matching_rep_id_when_filtering_by_specific_rep_id(
        self, mother
    ):
        strategy = GrupoKStoreManagerStrategy(
            target_period="2025-11-01",
            role_filter="Jefe de Tienda",
            rep_id_filter="13215149-0"
        )
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert result['Rep ID'].iloc[0] == '13215149-0'


class TestPeriodFiltering(TestGrupoKStoreManagerStrategy):

    def test_should_exclude_october_sales_when_target_period_is_november(
        self, mother
    ):
        strategy = GrupoKStoreManagerStrategy(
            target_period="2025-11-01",
            role_filter="Jefe de Tienda"
        )
        data = mother.create_data_with_attrs(
            mother.mixed_role_employees(),
            mother.sales_with_different_periods(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        iquique_row = result[result['Sucursal'] == 'Iquique'].iloc[0]
        assert iquique_row['Total Ventas'] == 25000000


class TestCommissionTiers(TestGrupoKStoreManagerStrategy):

    def test_should_return_700000_when_sales_at_30m(self, strategy, mother):
        tiers = mother.commission_tiers()
        assert strategy._get_fixed_commission(30000000, tiers) == 700000

    def test_should_return_700000_when_sales_at_40m(self, strategy, mother):
        tiers = mother.commission_tiers()
        assert strategy._get_fixed_commission(40000000, tiers) == 700000

    def test_should_return_820000_when_sales_at_40m_plus_one(self, strategy, mother):
        tiers = mother.commission_tiers()
        assert strategy._get_fixed_commission(40000001, tiers) == 820000

    def test_should_return_820000_when_sales_at_56m(self, strategy, mother):
        tiers = mother.commission_tiers()
        assert strategy._get_fixed_commission(56000000, tiers) == 820000

    def test_should_return_940000_when_sales_at_56m_plus_one(self, strategy, mother):
        tiers = mother.commission_tiers()
        assert strategy._get_fixed_commission(56000001, tiers) == 940000

    def test_should_return_1060000_when_sales_at_72m_plus_one(self, strategy, mother):
        tiers = mother.commission_tiers()
        assert strategy._get_fixed_commission(72000001, tiers) == 1060000

    def test_should_return_1060000_when_sales_at_200m(self, strategy, mother):
        tiers = mother.commission_tiers()
        assert strategy._get_fixed_commission(200000000, tiers) == 1060000


class TestStoreNameNormalization(TestGrupoKStoreManagerStrategy):

    def test_should_remove_accents_when_normalizing_la_fabrica(self, strategy):
        assert strategy._normalize_store_name("La Fábrica") == "la fabrica"

    def test_should_remove_accents_when_normalizing_vina_del_mar(self, strategy):
        assert strategy._normalize_store_name("Viña del Mar") == "vina del mar"

    def test_should_remove_accents_when_normalizing_temuco(self, strategy):
        assert strategy._normalize_store_name("Temúco") == "temuco"

    def test_should_return_empty_string_when_store_name_is_empty(self, strategy):
        assert strategy._normalize_store_name("") == ""

    def test_should_return_empty_string_when_store_name_is_none(self, strategy):
        assert strategy._normalize_store_name(None) == ""


class TestRutSanitization(TestGrupoKStoreManagerStrategy):

    def test_should_remove_dots_when_sanitizing_rut(self, strategy):
        assert strategy._sanitize_rut("13.215.149-0") == "13215149-0"

    def test_should_remove_spaces_when_sanitizing_rut(self, strategy):
        assert strategy._sanitize_rut(" 17190840-K ") == "17190840-K"

    def test_should_uppercase_verification_digit_when_sanitizing_rut(self, strategy):
        assert strategy._sanitize_rut("17190840-k") == "17190840-K"


class TestManagerWithoutStore(TestGrupoKStoreManagerStrategy):

    def test_should_return_empty_dataframe_when_manager_has_no_store_location(
        self, strategy, mother
    ):
        data = mother.create_data_with_attrs(
            mother.manager_without_store(),
            mother.sales_with_multiple_stores(),
            mother.commission_tiers()
        )

        result = strategy.calculate_commission(data)

        assert result.empty
