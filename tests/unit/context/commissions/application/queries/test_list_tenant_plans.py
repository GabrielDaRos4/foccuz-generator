from src.context.commissions.application.queries import ListTenantPlansHandler, ListTenantPlansQuery
from tests.mocks.commissions.repositories.mock_tenant_repository import MockTenantRepository
from tests.mothers.commissions.domain.aggregates_mother import PlanMother, TenantMother


class TestListTenantPlansHandler:

    def test_should_return_empty_when_tenant_not_found(self):
        repo = MockTenantRepository()
        handler = ListTenantPlansHandler(repo)

        result = handler.handle(ListTenantPlansQuery(tenant_id="UNKNOWN"))

        assert result == []

    def test_should_return_executable_plans_by_default(self):
        tenant = TenantMother.active(tenant_id="T1")
        active_plan = PlanMother.active(plan_id="PLAN_1", tenant_id="T1")
        inactive_plan = PlanMother.inactive(plan_id="PLAN_2", tenant_id="T1")
        tenant.add_plan(active_plan)
        tenant.add_plan(inactive_plan)
        repo = MockTenantRepository([tenant])
        handler = ListTenantPlansHandler(repo)

        result = handler.handle(ListTenantPlansQuery(tenant_id="T1"))

        assert len(result) == 1
        assert result[0].id == "PLAN_1"

    def test_should_return_all_plans_when_only_executable_false(self):
        tenant = TenantMother.active(tenant_id="T1")
        active_plan = PlanMother.active(plan_id="PLAN_1", tenant_id="T1")
        inactive_plan = PlanMother.inactive(plan_id="PLAN_2", tenant_id="T1")
        tenant.add_plan(active_plan)
        tenant.add_plan(inactive_plan)
        repo = MockTenantRepository([tenant])
        handler = ListTenantPlansHandler(repo)

        result = handler.handle(ListTenantPlansQuery(tenant_id="T1", only_executable=False))

        assert len(result) == 2
