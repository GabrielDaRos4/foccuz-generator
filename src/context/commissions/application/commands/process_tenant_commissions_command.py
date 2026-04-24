from dataclasses import dataclass

from src.context.shared.domain.cqrs import Command


@dataclass
class ProcessTenantCommissionsCommand(Command):
    tenant_id: str
    plan_ids: list[str] | None = None
    target_period: str | None = None
