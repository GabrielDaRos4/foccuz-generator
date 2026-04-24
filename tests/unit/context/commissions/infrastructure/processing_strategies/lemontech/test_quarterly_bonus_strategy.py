import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.lemontech.quarterly_bonus_strategy import (
    MIN_MONTHLY_COMPLIANCE,
    QUARTERLY_BONUS_RATE,
    LemontechQuarterlyBonusStrategy,
)


class TestLemontechQuarterlyBonusStrategy:

    @pytest.fixture
    def strategy(self):
        return LemontechQuarterlyBonusStrategy(target_period="2025-03-31")

    @pytest.fixture
    def sample_deals_q1(self):
        return pd.DataFrame({
            'id': ['1', '2', '3', '4', '5', '6'],
            'ownerRepId': ['123', '123', '123', '456', '456', '456'],
            'Tipo de Venta': ['New'] * 6,
            'closeDate': pd.to_datetime([
                '2025-01-15', '2025-02-15', '2025-03-15',
                '2025-01-10', '2025-02-10', '2025-03-10'
            ]),
            'name': ['Deal A', 'Deal B', 'Deal C', 'Deal D', 'Deal E', 'Deal F'],
            'Amount in company currency': ['1000', '1000', '1000', '500', '500', '500'],
            'pipelineLabel': ['Sales'] * 6,
            'Tipo de Cobro': ['Mensual'] * 6,
            'Opp Type': ['New Business'] * 6,
        })

    @pytest.fixture
    def sample_goals_q1(self):
        return pd.DataFrame({
            'Rep ID': ['123', '123', '123', '456', '456', '456'],
            'Nombre': ['John Doe'] * 3 + ['Jane Smith'] * 3,
            'Fecha': pd.to_datetime([
                '2025-01-01', '2025-02-01', '2025-03-01',
                '2025-01-01', '2025-02-01', '2025-03-01'
            ]),
            'Meta': ['1000', '1000', '1000', '1000', '1000', '1000'],
        })

    def _create_data_with_goals(
        self,
        deals: pd.DataFrame,
        goals: pd.DataFrame
    ) -> pd.DataFrame:
        result = deals.copy()
        result.attrs['goals'] = goals
        return result


class TestCalculateCommission(TestLemontechQuarterlyBonusStrategy):

    def test_should_return_dataframe_with_required_columns(
        self, strategy, sample_deals_q1, sample_goals_q1
    ):
        data = self._create_data_with_goals(sample_deals_q1, sample_goals_q1)

        result = strategy.calculate_commission(data)

        assert 'Fecha' in result.columns
        assert 'Rep ID' in result.columns
        assert 'ID Transacción' in result.columns
        assert 'Comisión' in result.columns

    def test_should_output_deals_with_zero_commission(
        self, strategy, sample_goals_q1
    ):
        deals = pd.DataFrame({
            'id': ['1', '2', '3'],
            'ownerRepId': ['123', '123', '123'],
            'Tipo de Venta': ['New'] * 3,
            'closeDate': pd.to_datetime([
                '2025-01-15', '2025-02-15', '2025-03-15'
            ]),
            'name': ['Deal A', 'Deal B', 'Deal C'],
            'Amount in company currency': ['1000', '1000', '1000'],
            'pipelineLabel': ['Sales'] * 3,
            'Tipo de Cobro': ['Mensual'] * 3,
            'Opp Type': ['New Business'] * 3,
        })
        data = self._create_data_with_goals(deals, sample_goals_q1)

        result = strategy.calculate_commission(data)

        assert len(result) == 3
        assert all(result['Comisión'] == 0)

    def test_should_filter_deals_by_reps_in_goals(
        self, strategy, sample_goals_q1
    ):
        deals = pd.DataFrame({
            'id': ['1', '2', '3'],
            'ownerRepId': ['123', '999', '123'],
            'Tipo de Venta': ['New'] * 3,
            'closeDate': pd.to_datetime([
                '2025-01-15', '2025-02-15', '2025-03-15'
            ]),
            'name': ['Deal A', 'Deal B', 'Deal C'],
            'Amount in company currency': ['1000', '500', '1000'],
            'pipelineLabel': ['Sales'] * 3,
            'Tipo de Cobro': ['Mensual'] * 3,
            'Opp Type': ['New Business'] * 3,
        })
        data = self._create_data_with_goals(deals, sample_goals_q1)

        result = strategy.calculate_commission(data)

        assert len(result) == 2
        assert all(result['Rep ID'] == '123')

    def test_should_include_quarterly_fecha(
        self, strategy, sample_goals_q1
    ):
        deals = pd.DataFrame({
            'id': ['1', '2', '3'],
            'ownerRepId': ['123', '123', '123'],
            'Tipo de Venta': ['New'] * 3,
            'closeDate': pd.to_datetime([
                '2025-01-15', '2025-02-15', '2025-03-15'
            ]),
            'name': ['Deal A', 'Deal B', 'Deal C'],
            'Amount in company currency': ['800', '800', '800'],
            'pipelineLabel': ['Sales'] * 3,
            'Tipo de Cobro': ['Mensual'] * 3,
            'Opp Type': ['New Business'] * 3,
        })
        data = self._create_data_with_goals(deals, sample_goals_q1)

        result = strategy.calculate_commission(data)

        assert len(result) == 3
        assert 'Fecha' in result.columns

    def test_should_return_empty_when_no_deals(self, strategy, sample_goals_q1):
        deals = pd.DataFrame()
        data = self._create_data_with_goals(deals, sample_goals_q1)

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_no_goals(self, strategy, sample_deals_q1):
        data = sample_deals_q1.copy()
        data.attrs['goals'] = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty


class TestFilterByRepId(TestLemontechQuarterlyBonusStrategy):

    def test_should_filter_results_by_rep_id(
        self, sample_deals_q1, sample_goals_q1
    ):
        strategy = LemontechQuarterlyBonusStrategy(
            target_period="2025-03-31",
            rep_id_filter="123"
        )
        data = self._create_data_with_goals(sample_deals_q1, sample_goals_q1)

        result = strategy.calculate_commission(data)

        assert all(result['Rep ID'] == '123')


class TestConstants:

    def test_min_monthly_compliance_is_60_percent(self):
        assert MIN_MONTHLY_COMPLIANCE == 0.6

    def test_quarterly_bonus_rate_is_20_percent(self):
        assert QUARTERLY_BONUS_RATE == 0.2
