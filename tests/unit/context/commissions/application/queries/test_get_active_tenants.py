from src.context.commissions.application.queries import GetActiveTenantsHandler, GetActiveTenantsQuery
from tests.mocks.commissions.repositories.mock_tenant_repository import MockTenantRepository
from tests.mothers.commissions.domain.aggregates_mother import TenantMother


class TestGetActiveTenantsHandler:

    def test_should_return_active_tenants(self):
        active = TenantMother.active(tenant_id="T1")
        inactive = TenantMother.inactive(tenant_id="T2")
        repo = MockTenantRepository([active, inactive])
        handler = GetActiveTenantsHandler(repo)

        result = handler.handle(GetActiveTenantsQuery())

        assert len(result) == 1
        assert result[0].id == "T1"

    def test_should_return_empty_when_no_active_tenants(self):
        repo = MockTenantRepository()
        handler = GetActiveTenantsHandler(repo)

        result = handler.handle(GetActiveTenantsQuery())

        assert result == []
