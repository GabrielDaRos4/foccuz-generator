from abc import ABC, abstractmethod

from src.context.commissions.domain.aggregates import Tenant


class TenantRepository(ABC):
    @abstractmethod
    def get_by_id(self, tenant_id: str) -> Tenant | None:
        pass

    @abstractmethod
    def get_all(self) -> list[Tenant]:
        pass

    @abstractmethod
    def get_active_tenants(self) -> list[Tenant]:
        pass
