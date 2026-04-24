import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.lemontech.monthly_commission_strategy import (
    COMPLIANCE_TIERS,
    FIXED_COMMISSION_DEALS,
    SKIP_ANNUAL_BONUS_DEALS,
    LemontechMonthlyCommissionStrategy,
)


class TestLemontechMonthlyCommissionStrategy:

    @pytest.fixture
    def strategy(self):
        return LemontechMonthlyCommissionStrategy(target_period="2025-01-01")

    @pytest.fixture
    def sample_deals(self):
        return pd.DataFrame({
            'id': ['1', '2', '3'],
            'ownerRepId': ['123', '123', '456'],
            'Tipo de Venta': ['New', 'New', 'Renewal'],
            'closeDate': pd.to_datetime(['2025-01-15', '2025-01-20', '2025-01-10']),
            'name': ['Deal A', 'Deal B', 'Deal C'],
            'Amount in company currency': ['1000', '2000', '1500'],
            'pipelineLabel': ['Sales', 'Sales', 'Sales'],
            'Tipo de Cobro': ['Mensual', 'Anual', 'Mensual'],
            'Opp Type': ['New Business', 'New Business', 'Add-on'],
        })

    @pytest.fixture
    def sample_goals(self):
        return pd.DataFrame({
            'Rep ID': ['123', '456'],
            'Nombre': ['John Doe', 'Jane Smith'],
            'Fecha': pd.to_datetime(['2025-01-01', '2025-01-01']),
            'Meta': ['2000', '1000'],
        })

    def _create_data_with_goals(
        self,
        deals: pd.DataFrame,
        goals: pd.DataFrame
    ) -> pd.DataFrame:
        result = deals.copy()
        result.attrs['goals'] = goals
        return result


class TestCalculateCommission(TestLemontechMonthlyCommissionStrategy):

    def test_should_return_dataframe_with_required_columns(
        self, strategy, sample_deals, sample_goals
    ):
        data = self._create_data_with_goals(sample_deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert 'Fecha' in result.columns
        assert 'Rep ID' in result.columns
        assert 'ID Transacción' in result.columns
        assert 'Comisión' in result.columns

    def test_should_calculate_commission_for_high_compliance(
        self, strategy, sample_goals
    ):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Deal A'],
            'Amount in company currency': ['3000'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Mensual'],
            'Opp Type': ['New Business'],
        })
        data = self._create_data_with_goals(deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['% Comision'].iloc[0] == 1.65
        assert 'Comisión' in result.columns

    def test_should_calculate_commission_for_exact_goal(
        self, strategy, sample_goals
    ):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Deal A'],
            'Amount in company currency': ['2000'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Mensual'],
            'Opp Type': ['New Business'],
        })
        data = self._create_data_with_goals(deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['% Comision'].iloc[0] == 1.35

    def test_should_return_zero_commission_for_low_compliance(
        self, strategy, sample_goals
    ):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Deal A'],
            'Amount in company currency': ['100'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Mensual'],
            'Opp Type': ['New Business'],
        })
        data = self._create_data_with_goals(deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['% Comision'].iloc[0] == 0

    def test_should_add_annual_bonus_for_annual_payment(self, strategy):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Deal A'],
            'Amount in company currency': ['2000'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Anual'],
            'Opp Type': ['New Business'],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Nombre': ['John Doe'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Pago por Anualidad'].iloc[0] > 0

    def test_should_not_add_annual_bonus_for_addon(self, strategy):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Deal A'],
            'Amount in company currency': ['2000'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Anual'],
            'Opp Type': ['Add-on'],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Nombre': ['John Doe'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Pago por Anualidad'].iloc[0] == 0

    def test_should_return_empty_when_no_deals(self, strategy, sample_goals):
        deals = pd.DataFrame()
        data = self._create_data_with_goals(deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_no_goals(self, strategy, sample_deals):
        data = sample_deals.copy()
        data.attrs['goals'] = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty


class TestAlbExceptions(TestLemontechMonthlyCommissionStrategy):

    def test_should_set_alb_payment_for_exception_deals(self, strategy):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['ALB ABOGADOS_MEX_CTFirms_100C_INB'],
            'Amount in company currency': ['5000'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Mensual'],
            'Opp Type': ['New Business'],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Nombre': ['John Doe'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Pago ALB (Omitido en Resultados)'].iloc[0] == 5000


class TestFilterByRepId(TestLemontechMonthlyCommissionStrategy):

    def test_should_filter_results_by_rep_id(self, sample_deals, sample_goals):
        strategy = LemontechMonthlyCommissionStrategy(
            target_period="2025-01-01",
            rep_id_filter="123"
        )
        data = self._create_data_with_goals(sample_deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert all(result['Rep ID'] == '123')


class TestSkipAnnualBonusDeals(TestLemontechMonthlyCommissionStrategy):

    def test_should_not_add_annual_bonus_for_skip_annual_deal(self, strategy):
        deal_name = SKIP_ANNUAL_BONUS_DEALS[0]
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': [deal_name],
            'Amount in company currency': ['2000'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Anual'],
            'Opp Type': ['New Business'],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Nombre': ['John Doe'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Pago por Anualidad'].iloc[0] == 0

    def test_should_still_add_annual_bonus_for_regular_deals(self, strategy):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Regular Deal Not In Skip List'],
            'Amount in company currency': ['2000'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Anual'],
            'Opp Type': ['New Business'],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Nombre': ['John Doe'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Pago por Anualidad'].iloc[0] > 0


class TestFixedCommissionDeals(TestLemontechMonthlyCommissionStrategy):

    def test_should_apply_fixed_commission_for_special_deal(self, strategy):
        deal_name = list(FIXED_COMMISSION_DEALS.keys())[0]
        expected_commission = FIXED_COMMISSION_DEALS[deal_name]
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': [deal_name],
            'Amount in company currency': ['500'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Mensual'],
            'Opp Type': ['New Business'],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Nombre': ['John Doe'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Monto comisión'].iloc[0] == expected_commission
        assert result['Comisión'].iloc[0] == expected_commission

    def test_should_use_normal_calculation_for_regular_deals(self, strategy):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'Tipo de Venta': ['New'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Regular Deal'],
            'Amount in company currency': ['2000'],
            'pipelineLabel': ['Sales'],
            'Tipo de Cobro': ['Mensual'],
            'Opp Type': ['New Business'],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Nombre': ['John Doe'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Monto comisión'].iloc[0] == 2700


class TestComplianceTiers(TestLemontechMonthlyCommissionStrategy):

    @pytest.mark.parametrize("compliance,expected_rate", [
        (1.3, 1.65),
        (1.2, 1.65),
        (1.15, 1.45),
        (1.1, 1.45),
        (1.05, 1.35),
        (1.0, 1.35),
        (0.9, 1.0),
        (0.8, 1.0),
        (0.7, 0.7),
        (0.65, 0.7),
        (0.62, 0.7),
        (0.6, 0.7),
        (0.55, 0.5),
        (0.5, 0.5),
        (0.45, 0.5),
        (0.4, 0.5),
        (0.3, 0.25),
        (0.2, 0.25),
        (0.1, 0),
    ])
    def test_commission_rate_for_compliance(self, compliance, expected_rate):
        def get_rate(c):
            for threshold, rate in COMPLIANCE_TIERS:
                if c >= threshold:
                    return rate
            return 0

        assert get_rate(compliance) == expected_rate
