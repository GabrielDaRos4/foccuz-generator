from src.context.commissions.application.queries import GetTenantHandler, GetTenantQuery
from tests.mocks.commissions.repositories.mock_tenant_repository import MockTenantRepository
from tests.mothers.commissions.domain.aggregates_mother import TenantMother


class TestGetTenantHandler:

    def test_should_return_tenant_when_found(self):
        tenant = TenantMother.active(tenant_id="T1")
        repo = MockTenantRepository([tenant])
        handler = GetTenantHandler(repo)

        result = handler.handle(GetTenantQuery(tenant_id="T1"))

        assert result.id == "T1"

    def test_should_return_none_when_not_found(self):
        repo = MockTenantRepository()
        handler = GetTenantHandler(repo)

        result = handler.handle(GetTenantQuery(tenant_id="UNKNOWN"))

        assert result is None
