from datetime import datetime

from src.context.commissions.application.dto import (
    PlanExecutionResult,
    TenantExecutionResult,
)


class TestTenantExecutionResult:

    def test_creates_result(self):
        plan_results = [
            PlanExecutionResult(
                plan_id="PLAN_001",
                plan_name="Test Plan",
                success=True,
                records_processed=100,
                total_commission=50000.0
            )
        ]
        result = TenantExecutionResult(
            tenant_id="TENANT1",
            tenant_name="Test Tenant",
            total_plans=1,
            successful_plans=1,
            failed_plans=0,
            plan_results=plan_results
        )

        assert result.tenant_id == "TENANT1"
        assert result.tenant_name == "Test Tenant"
        assert result.total_plans == 1
        assert result.successful_plans == 1

    def test_success_rate_property(self):
        result = TenantExecutionResult(
            tenant_id="TENANT1",
            tenant_name="Test",
            total_plans=4,
            successful_plans=3,
            failed_plans=1,
            plan_results=[]
        )

        assert result.success_rate == 75.0

    def test_success_rate_zero_when_no_plans(self):
        result = TenantExecutionResult(
            tenant_id="TENANT1",
            tenant_name="Test",
            total_plans=0,
            successful_plans=0,
            failed_plans=0,
            plan_results=[]
        )

        assert result.success_rate == 0.0

    def test_sets_timestamp_automatically(self):
        result = TenantExecutionResult(
            tenant_id="TENANT1",
            tenant_name="Test",
            total_plans=0,
            successful_plans=0,
            failed_plans=0,
            plan_results=[]
        )

        assert result.timestamp is not None
        assert isinstance(result.timestamp, datetime)
