from dataclasses import dataclass


@dataclass(frozen=True)
class BonusConfig:
    min_sales: int
    bonus_amount: float
