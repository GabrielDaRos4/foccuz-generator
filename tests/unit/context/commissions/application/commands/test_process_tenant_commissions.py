from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.context.commissions.application.commands import (
    ProcessTenantCommissionsCommand,
    ProcessTenantCommissionsHandler,
)
from src.context.commissions.application.dto import PlanExecutionResult
from src.context.commissions.domain.exceptions import InvalidTenantError
from tests.mocks.commissions.repositories.mock_tenant_repository import MockTenantRepository
from tests.mothers.commissions.domain.aggregates_mother import PlanMother, TenantMother


def _make_plan_handler(success=True, records=5, commission=1000.0):
    handler = MagicMock()
    result = PlanExecutionResult(
        plan_id="PLAN_001",
        plan_name="Test Plan",
        success=success,
        records_processed=records,
        total_commission=commission,
    )
    handler.handle_with_data.return_value = (result, pd.DataFrame({"a": range(records)}))
    return handler


class TestProcessTenantCommissionsHandler:

    def test_should_raise_when_tenant_not_found(self):
        repo = MockTenantRepository()
        handler = ProcessTenantCommissionsHandler(repo, _make_plan_handler())

        with pytest.raises(InvalidTenantError, match="Tenant not found"):
            handler.handle(ProcessTenantCommissionsCommand(tenant_id="UNKNOWN"))

    def test_should_raise_when_tenant_inactive(self):
        tenant = TenantMother.inactive(tenant_id="T1")
        repo = MockTenantRepository([tenant])
        handler = ProcessTenantCommissionsHandler(repo, _make_plan_handler())

        with pytest.raises(InvalidTenantError, match="not active"):
            handler.handle(ProcessTenantCommissionsCommand(tenant_id="T1"))

    def test_should_return_empty_result_when_no_executable_plans(self):
        tenant = TenantMother.active(tenant_id="T1")
        repo = MockTenantRepository([tenant])
        handler = ProcessTenantCommissionsHandler(repo, _make_plan_handler())

        result = handler.handle(ProcessTenantCommissionsCommand(tenant_id="T1"))

        assert result.total_plans == 0
        assert result.successful_plans == 0

    def test_should_process_all_executable_plans(self):
        tenant = TenantMother.active(tenant_id="T1")
        plan = PlanMother.active(plan_id="PLAN_1", tenant_id="T1")
        tenant.add_plan(plan)
        repo = MockTenantRepository([tenant])
        plan_handler = _make_plan_handler()
        handler = ProcessTenantCommissionsHandler(repo, plan_handler)

        result = handler.handle(ProcessTenantCommissionsCommand(tenant_id="T1"))

        assert result.total_plans == 1
        assert result.successful_plans == 1
        assert plan_handler.handle_with_data.called

    def test_should_process_only_requested_plan_ids(self):
        tenant = TenantMother.active(tenant_id="T1")
        plan1 = PlanMother.active(plan_id="PLAN_1", tenant_id="T1")
        plan2 = PlanMother.active(plan_id="PLAN_2", name="Plan 2", tenant_id="T1")
        tenant.add_plan(plan1)
        tenant.add_plan(plan2)
        repo = MockTenantRepository([tenant])
        plan_handler = _make_plan_handler()
        handler = ProcessTenantCommissionsHandler(repo, plan_handler)

        result = handler.handle(ProcessTenantCommissionsCommand(
            tenant_id="T1", plan_ids=["PLAN_1"]
        ))

        assert result.total_plans == 1
        assert plan_handler.handle_with_data.call_count == 1

    def test_should_count_failed_plans(self):
        tenant = TenantMother.active(tenant_id="T1")
        plan = PlanMother.active(plan_id="PLAN_1", tenant_id="T1")
        tenant.add_plan(plan)
        repo = MockTenantRepository([tenant])
        plan_handler = _make_plan_handler(success=False, records=0, commission=0)
        handler = ProcessTenantCommissionsHandler(repo, plan_handler)

        result = handler.handle(ProcessTenantCommissionsCommand(tenant_id="T1"))

        assert result.failed_plans == 1

    def test_should_cache_plan_results_for_dependencies(self):
        tenant = TenantMother.active(tenant_id="T1")
        dep_plan = PlanMother.active(plan_id="DEP", tenant_id="T1")
        main_plan = PlanMother.active(plan_id="MAIN", name="Main", tenant_id="T1")
        main_plan.depends_on = ["DEP"]
        tenant.add_plan(dep_plan)
        tenant.add_plan(main_plan)
        repo = MockTenantRepository([tenant])
        plan_handler = _make_plan_handler()
        handler = ProcessTenantCommissionsHandler(repo, plan_handler)

        result = handler.handle(ProcessTenantCommissionsCommand(tenant_id="T1"))

        assert result.total_plans == 2
        assert plan_handler.handle_with_data.call_count == 2

    def test_should_use_data_repo_cache_session_when_available(self):
        tenant = TenantMother.active(tenant_id="T1")
        plan = PlanMother.active(plan_id="PLAN_1", tenant_id="T1")
        tenant.add_plan(plan)
        repo = MockTenantRepository([tenant])
        data_repo = MagicMock()
        data_repo.start_cache_session = MagicMock()
        data_repo.end_cache_session = MagicMock(return_value={"hits": 1, "misses": 2, "hit_rate": 0.33})
        plan_handler = _make_plan_handler()
        handler = ProcessTenantCommissionsHandler(repo, plan_handler, data_repo=data_repo)

        handler.handle(ProcessTenantCommissionsCommand(tenant_id="T1"))

        data_repo.start_cache_session.assert_called_once_with(session_id="T1")
        data_repo.end_cache_session.assert_called_once()

    def test_should_pass_target_period_to_plan_handler(self):
        tenant = TenantMother.active(tenant_id="T1")
        plan = PlanMother.active(plan_id="PLAN_1", tenant_id="T1")
        tenant.add_plan(plan)
        repo = MockTenantRepository([tenant])
        plan_handler = _make_plan_handler()
        handler = ProcessTenantCommissionsHandler(repo, plan_handler)

        handler.handle(ProcessTenantCommissionsCommand(
            tenant_id="T1", target_period="2026-04"
        ))

        call_args = plan_handler.handle_with_data.call_args[0][0]
        assert call_args.target_period == "2026-04"

    def test_should_handle_circular_dependency(self):
        tenant = TenantMother.active(tenant_id="T1")
        plan_a = PlanMother.active(plan_id="A", name="Plan A", tenant_id="T1")
        plan_b = PlanMother.active(plan_id="B", name="Plan B", tenant_id="T1")
        plan_a.depends_on = ["B"]
        plan_b.depends_on = ["A"]
        tenant.add_plan(plan_a)
        tenant.add_plan(plan_b)
        repo = MockTenantRepository([tenant])
        plan_handler = _make_plan_handler()
        handler = ProcessTenantCommissionsHandler(repo, plan_handler)

        result = handler.handle(ProcessTenantCommissionsCommand(tenant_id="T1"))

        assert result.total_plans == 2

    def test_should_warn_on_missing_dependency(self):
        tenant = TenantMother.active(tenant_id="T1")
        plan = PlanMother.active(plan_id="MAIN", tenant_id="T1")
        plan.depends_on = ["NONEXISTENT"]
        tenant.add_plan(plan)
        repo = MockTenantRepository([tenant])
        plan_handler = _make_plan_handler()
        handler = ProcessTenantCommissionsHandler(repo, plan_handler)

        result = handler.handle(ProcessTenantCommissionsCommand(tenant_id="T1"))

        assert result.total_plans == 1
        assert plan_handler.handle_with_data.call_count == 1

    def test_should_add_dependency_plans_to_execution(self):
        tenant = TenantMother.active(tenant_id="T1")
        dep = PlanMother.active(plan_id="DEP", tenant_id="T1")
        main = PlanMother.active(plan_id="MAIN", name="Main", tenant_id="T1")
        main.depends_on = ["DEP"]
        tenant.add_plan(dep)
        tenant.add_plan(main)
        repo = MockTenantRepository([tenant])
        plan_handler = _make_plan_handler()
        handler = ProcessTenantCommissionsHandler(repo, plan_handler)

        handler.handle(ProcessTenantCommissionsCommand(
            tenant_id="T1", plan_ids=["MAIN"]
        ))

        assert plan_handler.handle_with_data.call_count == 2
