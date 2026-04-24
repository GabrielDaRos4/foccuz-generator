from dataclasses import dataclass, field

from src.context.commissions.domain.exceptions import InvalidPlanError, InvalidTenantError

from .plan import Plan


@dataclass
class Tenant:
    id: str
    name: str
    gsheet_id: str
    plans: list[Plan] = field(default_factory=list)
    active: bool = True

    def __post_init__(self):
        if not self.id:
            raise InvalidTenantError("Tenant ID cannot be empty")
        if not self.name:
            raise InvalidTenantError("Tenant name cannot be empty")
        if not self.gsheet_id:
            raise InvalidTenantError("Google Sheet ID cannot be empty")

    def add_plan(self, plan: Plan) -> None:
        if plan.tenant_id != self.id:
            raise InvalidPlanError(f"Plan belongs to tenant {plan.tenant_id}, not {self.id}")

        if self.get_plan(plan.id):
            raise InvalidPlanError(f"Plan {plan.id} already exists in tenant {self.id}")

        self.plans.append(plan)

    def get_plan(self, plan_id: str) -> Plan | None:
        return next((p for p in self.plans if p.id == plan_id), None)

    def get_executable_plans(self) -> list[Plan]:
        if not self.active:
            return []
        return [plan for plan in self.plans if plan.is_executable()]

    def deactivate(self) -> None:
        self.active = False
        for plan in self.plans:
            plan.deactivate()

    def activate(self) -> None:
        self.active = True
