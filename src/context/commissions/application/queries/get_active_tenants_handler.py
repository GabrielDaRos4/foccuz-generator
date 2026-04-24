from src.context.commissions.application.queries.get_active_tenants_query import (
    GetActiveTenantsQuery,
)
from src.context.commissions.domain.aggregates import Tenant
from src.context.commissions.domain.repositories import TenantRepository
from src.context.shared.domain.cqrs import QueryHandler


class GetActiveTenantsHandler(QueryHandler[list[Tenant]]):
    def __init__(self, tenant_repo: TenantRepository):
        self._tenant_repo = tenant_repo

    def handle(self, query: GetActiveTenantsQuery) -> list[Tenant]:
        return self._tenant_repo.get_active_tenants()
