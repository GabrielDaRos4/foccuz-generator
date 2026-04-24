from src.context.commissions.domain.aggregates import Tenant
from src.context.commissions.domain.repositories import TenantRepository


class MockTenantRepository(TenantRepository):

    def __init__(self, tenants: list[Tenant] = None):
        self._tenants: dict[str, Tenant] = {}
        if tenants:
            for tenant in tenants:
                self._tenants[tenant.id] = tenant

    def get_by_id(self, tenant_id: str) -> Tenant | None:
        return self._tenants.get(tenant_id)

    def get_all(self) -> list[Tenant]:
        return list(self._tenants.values())

    def get_active_tenants(self) -> list[Tenant]:
        return [t for t in self._tenants.values() if t.active]

    def add(self, tenant: Tenant) -> None:
        self._tenants[tenant.id] = tenant

    def clear(self) -> None:
        self._tenants.clear()
