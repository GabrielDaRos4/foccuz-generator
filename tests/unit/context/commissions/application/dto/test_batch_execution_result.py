from datetime import datetime

from src.context.commissions.application.dto import (
    BatchExecutionResult,
    TenantExecutionResult,
)


class TestBatchExecutionResult:

    def test_creates_result(self):
        tenant_results = [
            TenantExecutionResult(
                tenant_id="TENANT1",
                tenant_name="Test Tenant",
                total_plans=2,
                successful_plans=2,
                failed_plans=0,
                plan_results=[]
            )
        ]
        result = BatchExecutionResult(
            total_tenants=1,
            successful_tenants=1,
            failed_tenants=0,
            total_plans=2,
            successful_plans=2,
            failed_plans=0,
            tenant_results=tenant_results,
            execution_time_seconds=1.5
        )

        assert result.total_tenants == 1
        assert result.successful_tenants == 1
        assert result.failed_tenants == 0
        assert result.total_plans == 2
        assert result.successful_plans == 2
        assert result.failed_plans == 0
        assert result.execution_time_seconds == 1.5
        assert len(result.tenant_results) == 1

    def test_tenant_success_rate_property(self):
        result = BatchExecutionResult(
            total_tenants=4,
            successful_tenants=3,
            failed_tenants=1,
            total_plans=8,
            successful_plans=6,
            failed_plans=2,
            tenant_results=[]
        )

        assert result.tenant_success_rate == 75.0

    def test_plan_success_rate_property(self):
        result = BatchExecutionResult(
            total_tenants=2,
            successful_tenants=2,
            failed_tenants=0,
            total_plans=10,
            successful_plans=8,
            failed_plans=2,
            tenant_results=[]
        )

        assert result.plan_success_rate == 80.0

    def test_tenant_success_rate_zero_when_no_tenants(self):
        result = BatchExecutionResult(
            total_tenants=0,
            successful_tenants=0,
            failed_tenants=0,
            total_plans=0,
            successful_plans=0,
            failed_plans=0,
            tenant_results=[]
        )

        assert result.tenant_success_rate == 0.0

    def test_plan_success_rate_zero_when_no_plans(self):
        result = BatchExecutionResult(
            total_tenants=1,
            successful_tenants=1,
            failed_tenants=0,
            total_plans=0,
            successful_plans=0,
            failed_plans=0,
            tenant_results=[]
        )

        assert result.plan_success_rate == 0.0

    def test_sets_timestamp_automatically(self):
        result = BatchExecutionResult(
            total_tenants=0,
            successful_tenants=0,
            failed_tenants=0,
            total_plans=0,
            successful_plans=0,
            failed_plans=0,
            tenant_results=[]
        )

        assert result.timestamp is not None
        assert isinstance(result.timestamp, datetime)

    def test_defaults_execution_time_to_zero(self):
        result = BatchExecutionResult(
            total_tenants=0,
            successful_tenants=0,
            failed_tenants=0,
            total_plans=0,
            successful_plans=0,
            failed_plans=0,
            tenant_results=[]
        )

        assert result.execution_time_seconds == 0.0
