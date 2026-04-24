import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.standard.tiered_commission import (
    TieredCommissionStrategy,
)


class TestTieredCommissionStrategy:

    def test_tiered_calculation(self):
        tiers = [
            {"min": 0, "max": 10000, "rate": 0.05},
            {"min": 10000, "max": 50000, "rate": 0.10},
            {"min": 50000, "max": None, "rate": 0.15}
        ]

        strategy = TieredCommissionStrategy(tiers=tiers)

        data = pd.DataFrame({
            'employee_id': ['EMP001', 'EMP002', 'EMP003'],
            'employee_name': ['Low Seller', 'Mid Seller', 'Top Seller'],
            'ventas': [5000, 25000, 100000]
        })

        result = strategy.calculate_commission(data)

        assert len(result) == 3
        assert 'tier' in result.columns
        assert 'comision' in result.columns

        assert result.loc[0, 'comision'] == 250.0
        assert result.loc[0, 'tier'] == 'Tier 1'

        assert result.loc[1, 'comision'] == 2500.0
        assert result.loc[1, 'tier'] == 'Tier 2'

        assert result.loc[2, 'comision'] == 15000.0
        assert result.loc[2, 'tier'] == 'Tier 3'
