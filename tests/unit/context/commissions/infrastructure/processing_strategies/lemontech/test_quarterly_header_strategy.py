import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.lemontech.quarterly_header_strategy import (
    MIN_MONTHLY_COMPLIANCE,
    QUARTERLY_BONUS_RATE,
    LemontechQuarterlyHeaderStrategy,
)


class TestLemontechQuarterlyHeaderStrategy:

    @pytest.fixture
    def strategy(self):
        return LemontechQuarterlyHeaderStrategy(target_period="2025-03-31")

    @pytest.fixture
    def sample_deals(self):
        return pd.DataFrame({
            'id': ['1', '2', '3'],
            'ownerRepId': ['123', '123', '123'],
            'closeDate': pd.to_datetime(['2025-01-15', '2025-02-15', '2025-03-15']),
            'name': ['Deal A', 'Deal B', 'Deal C'],
            'Amount in company currency': ['1000', '1000', '1000'],
        })

    @pytest.fixture
    def sample_goals(self):
        return pd.DataFrame({
            'Rep ID': ['123', '123', '123'],
            'Fecha': pd.to_datetime(['2025-01-01', '2025-02-01', '2025-03-01']),
            'Meta': ['1000', '1000', '1000'],
        })

    def _create_data_with_goals(
        self,
        deals: pd.DataFrame,
        goals: pd.DataFrame
    ) -> pd.DataFrame:
        result = deals.copy()
        result.attrs['goals'] = goals
        return result


class TestCalculateCommission(TestLemontechQuarterlyHeaderStrategy):

    def test_should_return_empty_when_data_is_empty(self, strategy):
        data = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_no_goals(self, strategy, sample_deals):
        data = sample_deals.copy()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_dataframe_with_required_columns(
        self, strategy, sample_deals, sample_goals
    ):
        data = self._create_data_with_goals(sample_deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert 'Rep ID' in result.columns
        assert 'Fecha' in result.columns
        assert 'Meta' in result.columns
        assert 'Cumplimiento' in result.columns
        assert 'Cumplimiento Todos los Meses' in result.columns
        assert 'Subtotal' in result.columns

    def test_should_calculate_quarterly_bonus_when_all_months_met(
        self, strategy, sample_deals, sample_goals
    ):
        data = self._create_data_with_goals(sample_deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        expected_subtotal = 3000 * QUARTERLY_BONUS_RATE
        assert result['Subtotal'].iloc[0] == expected_subtotal

    def test_should_not_give_bonus_when_not_all_months_met(self, strategy, sample_goals):
        deals = pd.DataFrame({
            'id': ['1', '2', '3'],
            'ownerRepId': ['123', '123', '123'],
            'closeDate': pd.to_datetime(['2025-01-15', '2025-02-15', '2025-03-15']),
            'name': ['Deal A', 'Deal B', 'Deal C'],
            'Amount in company currency': ['1000', '500', '1000'],
        })
        data = self._create_data_with_goals(deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert result['Subtotal'].iloc[0] == 0

    def test_should_filter_by_rep_id_when_specified(self, sample_deals, sample_goals):
        strategy = LemontechQuarterlyHeaderStrategy(
            target_period="2025-03-31",
            rep_id_filter="123"
        )
        goals = pd.DataFrame({
            'Rep ID': ['123', '123', '123', '456', '456', '456'],
            'Fecha': pd.to_datetime([
                '2025-01-01', '2025-02-01', '2025-03-01',
                '2025-01-01', '2025-02-01', '2025-03-01'
            ]),
            'Meta': ['1000', '1000', '1000', '1000', '1000', '1000'],
        })
        deals = pd.DataFrame({
            'id': ['1', '2', '3', '4', '5', '6'],
            'ownerRepId': ['123', '123', '123', '456', '456', '456'],
            'closeDate': pd.to_datetime([
                '2025-01-15', '2025-02-15', '2025-03-15',
                '2025-01-15', '2025-02-15', '2025-03-15'
            ]),
            'name': ['A', 'B', 'C', 'D', 'E', 'F'],
            'Amount in company currency': ['1000', '1000', '1000', '1000', '1000', '1000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Rep ID'].iloc[0] == '123'


class TestCalculateMonthlyCompliance(TestLemontechQuarterlyHeaderStrategy):

    def test_should_mark_met_minimum_when_above_threshold(self, strategy):
        deals = pd.DataFrame({
            'ownerRepId': ['123'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Amount in company currency': [800],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': [1000],
        })

        result = strategy._calculate_monthly_compliance(deals, goals)

        assert result['met_minimum'].iloc[0] == 1

    def test_should_not_mark_met_minimum_when_below_threshold(self, strategy):
        deals = pd.DataFrame({
            'ownerRepId': ['123'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Amount in company currency': [500],
        })
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': [1000],
        })

        result = strategy._calculate_monthly_compliance(deals, goals)

        assert result['met_minimum'].iloc[0] == 0


class TestCalculateQuarterlyEligibility(TestLemontechQuarterlyHeaderStrategy):

    def test_should_set_all_months_met_true_when_three_months_met(self, strategy):
        monthly_compliance = pd.DataFrame({
            'ownerRepId': ['123', '123', '123'],
            'Quarter': pd.to_datetime(['2025-03-31', '2025-03-31', '2025-03-31']),
            'met_minimum': [1, 1, 1],
        })

        result = strategy._calculate_quarterly_eligibility(monthly_compliance)

        assert result['Cumplimiento Todos los Meses'].iloc[0]

    def test_should_set_all_months_met_false_when_not_three_months_met(self, strategy):
        monthly_compliance = pd.DataFrame({
            'ownerRepId': ['123', '123', '123'],
            'Quarter': pd.to_datetime(['2025-03-31', '2025-03-31', '2025-03-31']),
            'met_minimum': [1, 0, 1],
        })

        result = strategy._calculate_quarterly_eligibility(monthly_compliance)

        assert not result['Cumplimiento Todos los Meses'].iloc[0]


class TestConstants:

    def test_min_monthly_compliance_is_60_percent(self):
        assert MIN_MONTHLY_COMPLIANCE == 0.6

    def test_quarterly_bonus_rate_is_20_percent(self):
        assert QUARTERLY_BONUS_RATE == 0.2
