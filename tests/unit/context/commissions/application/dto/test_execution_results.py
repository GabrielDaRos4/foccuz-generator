from datetime import datetime

from src.context.commissions.application.dto import (
    BatchExecutionResult,
    PlanExecutionResult,
    TenantExecutionResult,
)


class TestPlanExecutionResult:

    def test_should_set_timestamp_when_none(self):
        result = PlanExecutionResult(
            plan_id="P1", plan_name="Plan", success=True,
            records_processed=10, total_commission=1000.0,
        )
        assert result.timestamp is not None

    def test_should_keep_explicit_timestamp(self):
        ts = datetime(2026, 1, 1)
        result = PlanExecutionResult(
            plan_id="P1", plan_name="Plan", success=True,
            records_processed=10, total_commission=1000.0, timestamp=ts,
        )
        assert result.timestamp == ts


class TestTenantExecutionResult:

    def test_should_set_timestamp_when_none(self):
        result = TenantExecutionResult(
            tenant_id="T1", tenant_name="Tenant", total_plans=1,
            successful_plans=1, failed_plans=0, plan_results=[],
        )
        assert result.timestamp is not None

    def test_should_keep_explicit_timestamp(self):
        ts = datetime(2026, 1, 1)
        result = TenantExecutionResult(
            tenant_id="T1", tenant_name="Tenant", total_plans=1,
            successful_plans=1, failed_plans=0, plan_results=[], timestamp=ts,
        )
        assert result.timestamp == ts


class TestBatchExecutionResult:

    def test_should_set_timestamp_when_none(self):
        result = BatchExecutionResult(
            total_tenants=1, successful_tenants=1, failed_tenants=0,
            total_plans=1, successful_plans=1, failed_plans=0, tenant_results=[],
        )
        assert result.timestamp is not None

    def test_should_keep_explicit_timestamp(self):
        ts = datetime(2026, 1, 1)
        result = BatchExecutionResult(
            total_tenants=1, successful_tenants=1, failed_tenants=0,
            total_plans=1, successful_plans=1, failed_plans=0,
            tenant_results=[], timestamp=ts,
        )
        assert result.timestamp == ts
