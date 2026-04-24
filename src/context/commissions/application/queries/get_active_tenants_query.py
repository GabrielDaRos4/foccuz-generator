from dataclasses import dataclass

from src.context.shared.domain.cqrs import Query


@dataclass
class GetActiveTenantsQuery(Query):
    pass
