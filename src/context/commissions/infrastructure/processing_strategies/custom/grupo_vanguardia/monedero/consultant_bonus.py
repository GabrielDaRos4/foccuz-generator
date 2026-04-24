from dataclasses import dataclass


@dataclass
class ConsultantBonus:
    consultant_id: str
    consultant_name: str
    consultant_email: str
    agency: str
    sales_count: int
    target: int
    qualifies: bool
    bonus: float
