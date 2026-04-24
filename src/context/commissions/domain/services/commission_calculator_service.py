import logging

import pandas as pd

from src.context.commissions.domain.aggregates import Plan
from src.context.commissions.domain.exceptions import PlanNotExecutableError
from src.context.commissions.domain.ports import ProcessingStrategy

logger = logging.getLogger(__name__)


class CommissionCalculatorService:
    @staticmethod
    def calculate(
        plan: Plan,
        data: pd.DataFrame,
        strategy: ProcessingStrategy
    ) -> pd.DataFrame:
        if not plan.is_executable():
            raise PlanNotExecutableError(
                f"Plan {plan.full_id} is not executable. "
                f"Active: {plan.active}, Valid: {plan.validity_period.is_currently_valid()}"
            )

        if data.empty:
            logger.warning(f"No data provided for plan {plan.full_id}")
            return pd.DataFrame()

        logger.info(f"Calculating commissions for plan {plan.full_id}")
        result = strategy.calculate_commission(data)
        logger.info(f"Successfully calculated {len(result)} commission records for plan {plan.full_id}")
        return result
