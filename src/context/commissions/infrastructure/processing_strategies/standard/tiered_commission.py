import logging

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy
from src.context.shared.infrastructure.validators import validate_required_columns

logger = logging.getLogger(__name__)

TierConfig = dict[str, float | int | None]


class TieredCommissionStrategy(ProcessingStrategy):
    def __init__(self, tiers: list[TierConfig]):
        self.tiers = sorted(tiers, key=lambda x: x['min'])
        logger.info(f"Initialized TieredCommissionStrategy with {len(tiers)} tiers")

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        required_columns = ['employee_id', 'employee_name', 'ventas']
        validate_required_columns(data, required_columns)

        logger.info(f"Calculating tiered commissions for {len(data)} employees")

        result = data.copy()
        result['comision'] = 0.0
        result['tier'] = ''

        for idx, row in result.iterrows():
            ventas: float = float(row['ventas'] or 0)
            tier_info = self._get_tier_for_amount(ventas)

            if tier_info:
                result.at[idx, 'comision'] = ventas * tier_info['rate']
                result.at[idx, 'tier'] = f"Tier {tier_info['index']+1}"

        result['comision'] = result['comision'].round(2)

        logger.info(
            f"Calculated tiered commissions: Total commission: ${result['comision'].sum():,.2f}"
        )

        return result[['employee_id', 'employee_name', 'ventas', 'tier', 'comision']]

    def _get_tier_for_amount(self, amount: float) -> TierConfig | None:
        for idx, tier in enumerate(self.tiers):
            min_val = tier['min']
            max_val = tier.get('max')

            if max_val is None:
                if amount >= min_val:
                    return {**tier, 'index': idx}
            else:
                if min_val <= amount < max_val:
                    return {**tier, 'index': idx}

        return None
