import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
    BonusCalculator,
    BonusConfig,
)


class TestBonusCalculator:

    @pytest.fixture
    def honda_calculator(self):
        return BonusCalculator(BonusConfig(min_sales=4, bonus_amount=8000))

    @pytest.fixture
    def acura_calculator(self):
        return BonusCalculator(BonusConfig(min_sales=3, bonus_amount=8000))

    @pytest.fixture
    def sample_sales(self):
        return pd.DataFrame({
            "IdConsultant": [1, 1, 1, 1, 2, 2, 2, 3],
            "Consultant_Name": ["Juan", "Juan", "Juan", "Juan", "Maria", "Maria", "Maria", "Pedro"],
            "Consultant_Mail": ["juan@test.com"] * 4 + ["maria@test.com"] * 3 + ["pedro@test.com"],
            "Agency": ["HONDA COLIMA"] * 8
        })


class TestCalculate(TestBonusCalculator):

    def test_qualifies_consultant_with_enough_sales(self, honda_calculator, sample_sales):
        result = honda_calculator.calculate(sample_sales)

        consultant_1 = next(r for r in result if r.consultant_id == "1")
        assert consultant_1.sales_count == 4
        assert consultant_1.qualifies is True
        assert consultant_1.bonus == 8000

    def test_does_not_qualify_consultant_below_target(self, honda_calculator, sample_sales):
        result = honda_calculator.calculate(sample_sales)

        consultant_2 = next(r for r in result if r.consultant_id == "2")
        assert consultant_2.sales_count == 3
        assert consultant_2.qualifies is False
        assert consultant_2.bonus == 0

        consultant_3 = next(r for r in result if r.consultant_id == "3")
        assert consultant_3.sales_count == 1
        assert consultant_3.qualifies is False
        assert consultant_3.bonus == 0

    def test_acura_requires_three_sales(self, acura_calculator, sample_sales):
        result = acura_calculator.calculate(sample_sales)

        consultant_2 = next(r for r in result if r.consultant_id == "2")
        assert consultant_2.sales_count == 3
        assert consultant_2.qualifies is True
        assert consultant_2.bonus == 8000

    def test_returns_correct_consultant_info(self, honda_calculator, sample_sales):
        result = honda_calculator.calculate(sample_sales)

        consultant_1 = next(r for r in result if r.consultant_id == "1")
        assert consultant_1.consultant_name == "Juan"
        assert consultant_1.consultant_email == "juan@test.com"
        assert consultant_1.agency == "HONDA COLIMA"

    def test_sets_target_from_config(self, honda_calculator, sample_sales):
        result = honda_calculator.calculate(sample_sales)

        for r in result:
            assert r.target == 4

    def test_raises_error_when_no_consultant_id(self, honda_calculator):
        df = pd.DataFrame({"Other": [1, 2, 3]})

        with pytest.raises(ValueError, match="No consultant ID column found"):
            honda_calculator.calculate(df)
