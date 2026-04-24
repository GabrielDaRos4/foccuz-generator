from unittest.mock import MagicMock

from src.context.commissions.application.commands import (
    ProcessAllTenantsCommand,
    ProcessAllTenantsHandler,
)
from src.context.commissions.application.dto import TenantExecutionResult
from tests.mocks.commissions.repositories.mock_tenant_repository import MockTenantRepository
from tests.mothers.commissions.domain.aggregates_mother import PlanMother, TenantMother


def _make_tenant_handler(success=True):
    handler = MagicMock()
    handler.handle.return_value = TenantExecutionResult(
        tenant_id="T1",
        tenant_name="Test",
        total_plans=1,
        successful_plans=1 if success else 0,
        failed_plans=0 if success else 1,
        plan_results=[],
        execution_time_seconds=0.1,
    )
    return handler


class TestProcessAllTenantsHandler:

    def test_should_return_empty_when_no_active_tenants(self):
        repo = MockTenantRepository()
        handler = ProcessAllTenantsHandler(repo, _make_tenant_handler())

        result = handler.handle(ProcessAllTenantsCommand())

        assert result.total_tenants == 0
        assert result.tenant_results == []

    def test_should_process_all_active_tenants(self):
        tenant1 = TenantMother.active(tenant_id="T1", name="Tenant 1")
        tenant2 = TenantMother.active(tenant_id="T2", name="Tenant 2")
        repo = MockTenantRepository([tenant1, tenant2])
        tenant_handler = _make_tenant_handler()
        handler = ProcessAllTenantsHandler(repo, tenant_handler)

        result = handler.handle(ProcessAllTenantsCommand())

        assert result.total_tenants == 2
        assert tenant_handler.handle.call_count == 2

    def test_should_skip_inactive_tenants(self):
        active = TenantMother.active(tenant_id="T1")
        inactive = TenantMother.inactive(tenant_id="T2")
        repo = MockTenantRepository([active, inactive])
        tenant_handler = _make_tenant_handler()
        handler = ProcessAllTenantsHandler(repo, tenant_handler)

        result = handler.handle(ProcessAllTenantsCommand())

        assert result.total_tenants == 1
        assert tenant_handler.handle.call_count == 1

    def test_should_handle_tenant_processing_error(self):
        tenant = TenantMother.active(tenant_id="T1")
        plan = PlanMother.active(plan_id="P1", tenant_id="T1")
        tenant.add_plan(plan)
        repo = MockTenantRepository([tenant])
        tenant_handler = MagicMock()
        tenant_handler.handle.side_effect = RuntimeError("Processing failed")
        handler = ProcessAllTenantsHandler(repo, tenant_handler)

        result = handler.handle(ProcessAllTenantsCommand())

        assert result.total_tenants == 1
        assert result.failed_tenants == 1
        assert result.tenant_results[0].failed_plans == 1

    def test_should_count_successful_and_failed_tenants(self):
        tenant1 = TenantMother.active(tenant_id="T1", name="Tenant 1")
        tenant2 = TenantMother.active(tenant_id="T2", name="Tenant 2")
        repo = MockTenantRepository([tenant1, tenant2])
        tenant_handler = MagicMock()

        success_result = TenantExecutionResult(
            tenant_id="T1", tenant_name="Tenant 1",
            total_plans=1, successful_plans=1, failed_plans=0,
            plan_results=[], execution_time_seconds=0.1
        )
        fail_result = TenantExecutionResult(
            tenant_id="T2", tenant_name="Tenant 2",
            total_plans=1, successful_plans=0, failed_plans=1,
            plan_results=[], execution_time_seconds=0.1
        )
        tenant_handler.handle.side_effect = [success_result, fail_result]
        handler = ProcessAllTenantsHandler(repo, tenant_handler)

        result = handler.handle(ProcessAllTenantsCommand())

        assert result.successful_tenants == 1
        assert result.failed_tenants == 1
        assert result.total_plans == 2
        assert result.successful_plans == 1
        assert result.failed_plans == 1

    def test_should_pass_target_period(self):
        tenant = TenantMother.active(tenant_id="T1")
        repo = MockTenantRepository([tenant])
        tenant_handler = _make_tenant_handler()
        handler = ProcessAllTenantsHandler(repo, tenant_handler)

        handler.handle(ProcessAllTenantsCommand(target_period="2026-04"))

        call_args = tenant_handler.handle.call_args[0][0]
        assert call_args.target_period == "2026-04"
