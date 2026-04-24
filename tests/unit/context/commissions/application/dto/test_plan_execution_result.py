from datetime import datetime

from src.context.commissions.application.dto import (
    PlanExecutionResult,
)


class TestPlanExecutionResult:

    def test_creates_result(self):
        result = PlanExecutionResult(
            plan_id="PLAN_001",
            plan_name="Test Plan",
            success=True,
            records_processed=100,
            total_commission=50000.0
        )

        assert result.plan_id == "PLAN_001"
        assert result.plan_name == "Test Plan"
        assert result.success is True
        assert result.records_processed == 100
        assert result.total_commission == 50000.0

    def test_defaults_error_message_to_none(self):
        result = PlanExecutionResult(
            plan_id="PLAN_001",
            plan_name="Test",
            success=True,
            records_processed=0,
            total_commission=0
        )

        assert result.error_message is None

    def test_sets_timestamp_automatically(self):
        result = PlanExecutionResult(
            plan_id="PLAN_001",
            plan_name="Test",
            success=True,
            records_processed=0,
            total_commission=0
        )

        assert result.timestamp is not None
        assert isinstance(result.timestamp, datetime)

    def test_stores_error_message(self):
        result = PlanExecutionResult(
            plan_id="PLAN_001",
            plan_name="Test",
            success=False,
            records_processed=0,
            total_commission=0,
            error_message="Test error"
        )

        assert result.error_message == "Test error"
