from dataclasses import dataclass

from src.context.shared.domain.cqrs import Query


@dataclass
class ListTenantPlansQuery(Query):
    tenant_id: str
    only_executable: bool = True
