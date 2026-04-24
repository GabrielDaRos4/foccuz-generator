from dataclasses import dataclass


@dataclass(frozen=True)
class LicensePlateBonusConfig:
    bonus_per_month: float = 15000
