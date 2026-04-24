from dataclasses import dataclass


@dataclass(frozen=True)
class CommissionConfig:
    discount_percentage: float
    max_factor: float
    new_client_bonus: float
    min_factor: float
