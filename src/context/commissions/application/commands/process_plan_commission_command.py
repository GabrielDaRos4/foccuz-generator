from dataclasses import dataclass, field

from src.context.commissions.domain.aggregates import Plan, Tenant
from src.context.shared.domain.cqrs import Command


@dataclass
class ProcessPlanCommissionCommand(Command):
    tenant: Tenant
    plan: Plan
    target_period: str | None = None
    dependency_results: dict = field(default_factory=dict)
