from datetime import datetime

from src.context.commissions.domain.events import (
    CommissionCalculated,
    CommissionExported,
    PlanProcessingFailed,
)
from src.context.shared.domain import DomainEvent


class TestDomainEvent:

    def test_domain_event_sets_occurred_at_automatically(self):
        class TestEvent(DomainEvent):
            pass

        before = datetime.now()
        event = TestEvent()
        after = datetime.now()

        assert event.occurred_at is not None
        assert before <= event.occurred_at <= after

    def test_domain_event_accepts_custom_occurred_at(self):
        class TestEvent(DomainEvent):
            pass

        custom_time = datetime(2025, 1, 1, 12, 0, 0)
        event = TestEvent(occurred_at=custom_time)

        assert event.occurred_at == custom_time


class TestCommissionCalculated:

    def test_create_commission_calculated_event(self):
        event = CommissionCalculated(
            tenant_id="COPEC",
            plan_id="PLAN_800",
            records_count=150,
            total_commission=25000.50
        )

        assert event.tenant_id == "COPEC"
        assert event.plan_id == "PLAN_800"
        assert event.records_count == 150
        assert event.total_commission == 25000.50
        assert event.occurred_at is not None

    def test_commission_calculated_with_custom_time(self):
        custom_time = datetime(2025, 10, 15, 14, 30, 0)
        event = CommissionCalculated(
            tenant_id="SCANIA",
            plan_id="PLAN_798",
            records_count=50,
            total_commission=100000.0,
            occurred_at=custom_time
        )

        assert event.occurred_at == custom_time

    def test_commission_calculated_default_values(self):
        event = CommissionCalculated()

        assert event.tenant_id == ""
        assert event.plan_id == ""
        assert event.records_count == 0
        assert event.total_commission == 0.0


class TestCommissionExported:

    def test_create_commission_exported_event(self):
        event = CommissionExported(
            tenant_id="COPEC",
            plan_id="PLAN_800",
            sheet_id="1abc123xyz",
            tab_name="PLAN_800",
            records_count=150
        )

        assert event.tenant_id == "COPEC"
        assert event.plan_id == "PLAN_800"
        assert event.sheet_id == "1abc123xyz"
        assert event.tab_name == "PLAN_800"
        assert event.records_count == 150
        assert event.occurred_at is not None

    def test_commission_exported_default_values(self):
        event = CommissionExported()

        assert event.tenant_id == ""
        assert event.plan_id == ""
        assert event.sheet_id == ""
        assert event.tab_name == ""
        assert event.records_count == 0


class TestPlanProcessingFailed:

    def test_create_plan_processing_failed_event(self):
        event = PlanProcessingFailed(
            tenant_id="COPEC",
            plan_id="PLAN_800",
            error_message="Data source not found",
            error_details={"source_id": "ventas", "type": "FileNotFoundError"}
        )

        assert event.tenant_id == "COPEC"
        assert event.plan_id == "PLAN_800"
        assert event.error_message == "Data source not found"
        assert event.error_details["source_id"] == "ventas"
        assert event.occurred_at is not None

    def test_plan_processing_failed_default_values(self):
        event = PlanProcessingFailed()

        assert event.tenant_id == ""
        assert event.plan_id == ""
        assert event.error_message == ""
        assert event.error_details == {}

    def test_plan_processing_failed_with_empty_error_details(self):
        event = PlanProcessingFailed(
            tenant_id="TEST",
            plan_id="PLAN_001",
            error_message="Unknown error"
        )

        assert event.error_details == {}
