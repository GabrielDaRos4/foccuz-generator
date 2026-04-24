from src.context.commissions.application.queries.get_tenant_query import GetTenantQuery
from src.context.commissions.domain.aggregates import Tenant
from src.context.commissions.domain.repositories import TenantRepository
from src.context.shared.domain.cqrs import QueryHandler


class GetTenantHandler(QueryHandler[Tenant | None]):
    def __init__(self, tenant_repo: TenantRepository):
        self._tenant_repo = tenant_repo

    def handle(self, query: GetTenantQuery) -> Tenant | None:
        return self._tenant_repo.get_by_id(query.tenant_id)
