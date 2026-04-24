from dataclasses import dataclass

from src.context.shared.domain.cqrs import Query


@dataclass
class GetTenantQuery(Query):
    tenant_id: str
