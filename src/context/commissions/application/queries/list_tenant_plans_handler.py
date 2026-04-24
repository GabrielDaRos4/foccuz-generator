from src.context.commissions.application.queries.list_tenant_plans_query import (
    ListTenantPlansQuery,
)
from src.context.commissions.domain.aggregates import Plan
from src.context.commissions.domain.repositories import TenantRepository
from src.context.shared.domain.cqrs import QueryHandler


class ListTenantPlansHandler(QueryHandler[list[Plan]]):
    def __init__(self, tenant_repo: TenantRepository):
        self._tenant_repo = tenant_repo

    def handle(self, query: ListTenantPlansQuery) -> list[Plan]:
        tenant = self._tenant_repo.get_by_id(query.tenant_id)
        if not tenant:
            return []

        if query.only_executable:
            return tenant.get_executable_plans()
        return tenant.plans
