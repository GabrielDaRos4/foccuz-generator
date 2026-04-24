from dataclasses import dataclass

from src.context.shared.domain.cqrs import Command


@dataclass
class ProcessAllTenantsCommand(Command):
    target_period: str | None = None
