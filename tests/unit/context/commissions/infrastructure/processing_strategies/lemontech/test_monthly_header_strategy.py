import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.lemontech.monthly_header_strategy import (
    LemontechMonthlyHeaderStrategy,
)


class TestLemontechMonthlyHeaderStrategy:

    @pytest.fixture
    def strategy(self):
        return LemontechMonthlyHeaderStrategy(target_period="2025-01-01")

    @pytest.fixture
    def sample_deals(self):
        return pd.DataFrame({
            'id': ['1', '2'],
            'ownerRepId': ['123', '123'],
            'closeDate': pd.to_datetime(['2025-01-15', '2025-01-20']),
            'name': ['Deal A', 'Deal B'],
            'Amount in company currency': ['1000', '2000'],
        })

    @pytest.fixture
    def sample_goals(self):
        return pd.DataFrame({
            'Rep ID': ['123'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })

    def _create_data_with_goals(
        self,
        deals: pd.DataFrame,
        goals: pd.DataFrame
    ) -> pd.DataFrame:
        result = deals.copy()
        result.attrs['goals'] = goals
        return result


class TestCalculateCommission(TestLemontechMonthlyHeaderStrategy):

    def test_should_return_empty_when_data_is_empty(self, strategy):
        data = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_no_goals(self, strategy, sample_deals):
        data = sample_deals.copy()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_goals_is_empty(self, strategy, sample_deals):
        data = self._create_data_with_goals(sample_deals, pd.DataFrame())

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
        assert '% Comision' in result.columns
        assert 'Subtotal' in result.columns

    def test_should_calculate_compliance_correctly(
        self, strategy, sample_goals
    ):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Deal A'],
            'Amount in company currency': ['3000'],
        })
        data = self._create_data_with_goals(deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Cumplimiento'].iloc[0] == 1.5

    def test_should_calculate_subtotal_based_on_commission_rate(
        self, strategy, sample_goals
    ):
        deals = pd.DataFrame({
            'id': ['1'],
            'ownerRepId': ['123'],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'name': ['Deal A'],
            'Amount in company currency': ['3000'],
        })
        data = self._create_data_with_goals(deals, sample_goals)

        result = strategy.calculate_commission(data)

        expected_subtotal = 3000 * 1.65
        assert result['Subtotal'].iloc[0] == expected_subtotal

    def test_should_filter_by_rep_id_when_specified(self, sample_deals, sample_goals):
        strategy = LemontechMonthlyHeaderStrategy(
            target_period="2025-01-01",
            rep_id_filter="123"
        )
        goals = pd.DataFrame({
            'Rep ID': ['123', '456'],
            'Fecha': pd.to_datetime(['2025-01-01', '2025-01-01']),
            'Meta': ['2000', '2000'],
        })
        deals = pd.DataFrame({
            'id': ['1', '2'],
            'ownerRepId': ['123', '456'],
            'closeDate': pd.to_datetime(['2025-01-15', '2025-01-15']),
            'name': ['Deal A', 'Deal B'],
            'Amount in company currency': ['3000', '3000'],
        })
        data = self._create_data_with_goals(deals, goals)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result['Rep ID'].iloc[0] == '123'


class TestPrepareDeals(TestLemontechMonthlyHeaderStrategy):

    def test_should_filter_deals_after_2025(self, strategy):
        deals = pd.DataFrame({
            'ownerRepId': ['123', '123'],
            'closeDate': pd.to_datetime(['2024-12-15', '2025-01-15']),
            'Amount in company currency': ['1000', '2000'],
        })

        result = strategy._prepare_deals(deals)

        assert len(result) == 1

    def test_should_convert_owner_rep_id_to_string(self, strategy):
        deals = pd.DataFrame({
            'ownerRepId': [123.0],
            'closeDate': pd.to_datetime(['2025-01-15']),
            'Amount in company currency': ['1000'],
        })

        result = strategy._prepare_deals(deals)

        assert result['ownerRepId'].dtype == object
        assert result['ownerRepId'].iloc[0] == '123'


class TestPrepareGoals(TestLemontechMonthlyHeaderStrategy):

    def test_should_convert_rep_id_to_string(self, strategy):
        goals = pd.DataFrame({
            'Rep ID': [123],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': [2000],
        })

        result = strategy._prepare_goals(goals)

        assert result['Rep ID'].dtype == object
        assert result['Rep ID'].iloc[0] == '123'

    def test_should_convert_meta_to_numeric(self, strategy):
        goals = pd.DataFrame({
            'Rep ID': ['123'],
            'Fecha': pd.to_datetime(['2025-01-01']),
            'Meta': ['2000'],
        })

        result = strategy._prepare_goals(goals)

        assert result['Meta'].iloc[0] == 2000.0


class TestFormatOutput(TestLemontechMonthlyHeaderStrategy):

    def test_should_include_column_types_in_attrs(
        self, strategy, sample_deals, sample_goals
    ):
        data = self._create_data_with_goals(sample_deals, sample_goals)

        result = strategy.calculate_commission(data)

        assert 'column_types' in result.attrs
        assert result.attrs['column_types']['Rep ID'] == 'text'
        assert result.attrs['column_types']['Subtotal'] == 'money'
