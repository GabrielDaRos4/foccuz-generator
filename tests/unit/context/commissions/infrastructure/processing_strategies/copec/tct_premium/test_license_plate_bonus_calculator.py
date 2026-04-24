import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    LicensePlateBonusCalculator,
    LicensePlateBonusConfig,
)


class TestLicensePlateBonusCalculator:

    @pytest.fixture
    def config(self):
        return LicensePlateBonusConfig(bonus_per_month=15000)

    @pytest.fixture
    def calculator(self, config):
        return LicensePlateBonusCalculator(config)

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "license_plate_normalized": ["ABC123", "DEF456"],
            "client_rut_complete": ["11111111-1", "22222222-2"],
            "rep_id": ["12345678-9", "87654321-K"],
        })


class TestCalculate(TestLicensePlateBonusCalculator):

    def test_calculates_bonus(self, calculator, sample_data):
        result = calculator.calculate(sample_data)

        assert "new_client_bonus" in result.columns
        assert all(result["new_client_bonus"] == 15000)

    def test_deduplicates_by_client_and_plate(self, calculator):
        df = pd.DataFrame({
            "license_plate_normalized": ["ABC123", "ABC123"],
            "client_rut_complete": ["11111111-1", "11111111-1"],
            "rep_id": ["12345678-9", "12345678-9"],
        })
        result = calculator.calculate(df)

        assert len(result) == 1

    def test_keeps_different_plates_same_client(self, calculator):
        df = pd.DataFrame({
            "license_plate_normalized": ["ABC123", "DEF456"],
            "client_rut_complete": ["11111111-1", "11111111-1"],
            "rep_id": ["12345678-9", "12345678-9"],
        })
        result = calculator.calculate(df)

        assert len(result) == 2

    def test_bonus_is_integer(self, calculator, sample_data):
        result = calculator.calculate(sample_data)

        assert result["new_client_bonus"].dtype in ["int64", "int32"]

    def test_custom_bonus_amount(self):
        config = LicensePlateBonusConfig(bonus_per_month=20000)
        calculator = LicensePlateBonusCalculator(config)
        df = pd.DataFrame({
            "license_plate_normalized": ["ABC123"],
            "client_rut_complete": ["11111111-1"],
            "rep_id": ["12345678-9"],
        })

        result = calculator.calculate(df)

        assert result["new_client_bonus"].iloc[0] == 20000
