import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client import (
    CommissionCalculator,
    CommissionConfig,
)


class TestCommissionCalculator:

    @pytest.fixture
    def config(self):
        return CommissionConfig(
            discount_percentage=0.08,
            max_factor=6.0,
            new_client_bonus=10000,
            min_factor=0.5
        )

    @pytest.fixture
    def calculator(self, config):
        return CommissionCalculator(config)

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "client_rut_complete": ["11111111-1", "22222222-2"],
            "volumen": [1000, 2000],
            "descuento": [100, 200],
            "gets_bonus": [True, False],
        })


class TestCalculate(TestCommissionCalculator):

    def test_calculates_total_commission(self, calculator, sample_data):
        result = calculator.calculate(sample_data)

        assert "total_commission" in result.columns
        assert all(result["total_commission"] >= 0)

    def test_applies_bonus_for_new_clients(self, calculator, sample_data):
        result = calculator.calculate(sample_data)

        client_with_bonus = result[result["client_rut_complete"] == "11111111-1"]
        assert client_with_bonus["new_client_bonus"].iloc[0] == 10000

    def test_no_bonus_for_existing_clients(self, calculator, sample_data):
        result = calculator.calculate(sample_data)

        client_without_bonus = result[result["client_rut_complete"] == "22222222-2"]
        assert client_without_bonus["new_client_bonus"].iloc[0] == 0

    def test_deduplicates_by_client_rut(self, calculator):
        df = pd.DataFrame({
            "client_rut_complete": ["11111111-1", "11111111-1"],
            "volumen": [1000, 500],
            "descuento": [100, 50],
            "gets_bonus": [True, True],
        })
        result = calculator.calculate(df)

        assert len(result) == 1
        assert result["volumen"].iloc[0] == 1000

    def test_sets_client_type_column(self, calculator, sample_data):
        result = calculator.calculate(sample_data)

        assert "client_type" in result.columns
        client_with_bonus = result[result["client_rut_complete"] == "11111111-1"]
        assert client_with_bonus["client_type"].iloc[0] == "SI"

    def test_respects_min_factor(self, calculator):
        df = pd.DataFrame({
            "client_rut_complete": ["11111111-1"],
            "volumen": [100],
            "descuento": [10000],
            "gets_bonus": [False],
        })
        result = calculator.calculate(df)

        assert result["unit_commission"].iloc[0] >= 0.5

    def test_total_commission_is_integer(self, calculator, sample_data):
        result = calculator.calculate(sample_data)

        assert result["total_commission"].dtype in ["int64", "int32"]
