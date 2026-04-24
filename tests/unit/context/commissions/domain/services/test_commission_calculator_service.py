import pandas as pd
import pytest

from src.context.commissions.domain.exceptions import PlanNotExecutableError
from src.context.commissions.domain.ports import ProcessingStrategy
from src.context.commissions.domain.services import CommissionCalculatorService
from tests.mothers.commissions.domain.aggregates_mother import PlanMother


class MockStrategy(ProcessingStrategy):
    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        result = data.copy()
        result["commission"] = 1000
        return result


class FailingStrategy(ProcessingStrategy):
    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        raise RuntimeError("Strategy failed")


class TestCommissionCalculatorService:

    def test_should_calculate_commission_successfully(self):
        plan = PlanMother.active()
        data = pd.DataFrame({"name": ["Alice", "Bob"]})
        strategy = MockStrategy()

        result = CommissionCalculatorService.calculate(plan, data, strategy)

        assert len(result) == 2
        assert "commission" in result.columns

    def test_should_raise_when_plan_not_executable(self):
        plan = PlanMother.inactive()
        data = pd.DataFrame({"name": ["Alice"]})
        strategy = MockStrategy()

        with pytest.raises(PlanNotExecutableError, match="not executable"):
            CommissionCalculatorService.calculate(plan, data, strategy)

    def test_should_return_empty_dataframe_when_data_empty(self):
        plan = PlanMother.active()
        data = pd.DataFrame()
        strategy = MockStrategy()

        result = CommissionCalculatorService.calculate(plan, data, strategy)

        assert result.empty

    def test_should_propagate_strategy_error(self):
        plan = PlanMother.active()
        data = pd.DataFrame({"name": ["Alice"]})
        strategy = FailingStrategy()

        with pytest.raises(RuntimeError, match="Strategy failed"):
            CommissionCalculatorService.calculate(plan, data, strategy)
