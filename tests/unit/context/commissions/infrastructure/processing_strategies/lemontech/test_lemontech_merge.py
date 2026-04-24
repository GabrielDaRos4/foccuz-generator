import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.lemontech.lemontech_merge import (
    lemontech_monthly_merge,
    lemontech_quarterly_merge,
)


class TestLemontechMonthlyMerge:

    @pytest.fixture
    def sample_deals(self):
        return pd.DataFrame({
            "id": ["1", "2"],
            "ownerRepId": ["123", "456"],
            "Amount in company currency": [1000, 2000],
        })

    @pytest.fixture
    def sample_goals(self):
        return pd.DataFrame({
            "Rep ID": ["123", "456"],
            "Meta": [1000, 2000],
        })


class TestMonthlyMerge(TestLemontechMonthlyMerge):

    def test_should_return_deals_dataframe(self, sample_deals, sample_goals):
        dataframes = {
            "deals": sample_deals,
            "goals": sample_goals,
        }

        result = lemontech_monthly_merge(dataframes)

        assert len(result) == 2
        assert "id" in result.columns

    def test_should_attach_goals_to_attrs(self, sample_deals, sample_goals):
        dataframes = {
            "deals": sample_deals,
            "goals": sample_goals,
        }

        result = lemontech_monthly_merge(dataframes)

        assert "goals" in result.attrs
        assert len(result.attrs["goals"]) == 2

    def test_should_raise_error_when_no_deals(self, sample_goals):
        dataframes = {
            "goals": sample_goals,
        }

        with pytest.raises(ValueError, match="No deals data found"):
            lemontech_monthly_merge(dataframes)

    def test_should_raise_error_when_no_goals(self, sample_deals):
        dataframes = {
            "deals": sample_deals,
        }

        with pytest.raises(ValueError, match="No goals data found"):
            lemontech_monthly_merge(dataframes)

    def test_should_raise_error_when_deals_is_empty(self, sample_goals):
        dataframes = {
            "deals": pd.DataFrame(),
            "goals": sample_goals,
        }

        with pytest.raises(ValueError, match="No deals data found"):
            lemontech_monthly_merge(dataframes)

    def test_should_include_target_period_from_config(
        self, sample_deals, sample_goals
    ):
        dataframes = {
            "deals": sample_deals,
            "goals": sample_goals,
        }
        config = {"target_period": "2025-01-01"}

        result = lemontech_monthly_merge(dataframes, config)

        assert result.attrs.get("target_period") == "2025-01-01"


class TestQuarterlyMerge(TestLemontechMonthlyMerge):

    def test_should_return_deals_dataframe(self, sample_deals, sample_goals):
        dataframes = {
            "deals": sample_deals,
            "goals": sample_goals,
        }

        result = lemontech_quarterly_merge(dataframes)

        assert len(result) == 2
        assert "id" in result.columns

    def test_should_attach_goals_to_attrs(self, sample_deals, sample_goals):
        dataframes = {
            "deals": sample_deals,
            "goals": sample_goals,
        }

        result = lemontech_quarterly_merge(dataframes)

        assert "goals" in result.attrs

    def test_should_raise_error_when_no_deals(self, sample_goals):
        dataframes = {
            "goals": sample_goals,
        }

        with pytest.raises(ValueError, match="No deals data found"):
            lemontech_quarterly_merge(dataframes)

    def test_should_raise_error_when_no_goals(self, sample_deals):
        dataframes = {
            "deals": sample_deals,
        }

        with pytest.raises(ValueError, match="No goals data found"):
            lemontech_quarterly_merge(dataframes)

    def test_should_include_target_period_from_config(
        self, sample_deals, sample_goals
    ):
        dataframes = {
            "deals": sample_deals,
            "goals": sample_goals,
        }
        config = {"target_period": "2025-03-31"}

        result = lemontech_quarterly_merge(dataframes, config)

        assert result.attrs.get("target_period") == "2025-03-31"
