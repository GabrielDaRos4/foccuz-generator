from datetime import datetime

import pytest

from src.context.commissions.domain.aggregates import Plan
from src.context.commissions.domain.exceptions import InvalidPlanError
from src.context.commissions.domain.value_objects import (
    DataSourceCollection,
    DataSourceConfig,
    OutputConfig,
    StrategyConfig,
    ValidityPeriod,
)


class TestPlan:

    @pytest.fixture
    def valid_data_sources(self):
        source = DataSourceConfig(
            source_id="default",
            source_type="csv",
            config={"path": "data.csv"}
        )
        return DataSourceCollection(sources=[source])

    @pytest.fixture
    def valid_output_config(self):
        return OutputConfig(sheet_id="sheet123", tab_name="Tab1")

    @pytest.fixture
    def valid_strategy_config(self):
        return StrategyConfig(
            module="src.context.commissions.infrastructure.strategies",
            class_name="TestStrategy",
            params={}
        )

    def test_create_valid_plan(self, valid_data_sources, valid_output_config, valid_strategy_config):
        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="TEST_TENANT",
            active=True,
            data_sources=valid_data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config
        )

        assert plan.id == "PLAN_001"
        assert plan.name == "Test Plan"
        assert plan.tenant_id == "TEST_TENANT"
        assert plan.active is True

    def test_plan_requires_id(self, valid_data_sources, valid_output_config, valid_strategy_config):
        with pytest.raises(InvalidPlanError, match="Plan ID cannot be empty"):
            Plan(
                id="",
                name="Test Plan",
                tenant_id="TEST",
                active=True,
                data_sources=valid_data_sources,
                output_config=valid_output_config,
                strategy_config=valid_strategy_config
            )

    def test_plan_requires_name(self, valid_data_sources, valid_output_config, valid_strategy_config):
        with pytest.raises(InvalidPlanError, match="Plan name cannot be empty"):
            Plan(
                id="PLAN_001",
                name="",
                tenant_id="TEST",
                active=True,
                data_sources=valid_data_sources,
                output_config=valid_output_config,
                strategy_config=valid_strategy_config
            )

    def test_plan_requires_tenant_id(self, valid_data_sources, valid_output_config, valid_strategy_config):
        with pytest.raises(InvalidPlanError, match="Tenant ID cannot be empty"):
            Plan(
                id="PLAN_001",
                name="Test Plan",
                tenant_id="",
                active=True,
                data_sources=valid_data_sources,
                output_config=valid_output_config,
                strategy_config=valid_strategy_config
            )

    def test_plan_full_id(self, valid_data_sources, valid_output_config, valid_strategy_config):
        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="COPEC",
            active=True,
            data_sources=valid_data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config
        )

        assert plan.full_id == "COPEC.PLAN_001"

    def test_plan_is_executable_when_active_and_valid(
            self, valid_data_sources, valid_output_config, valid_strategy_config
    ):
        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="TEST",
            active=True,
            data_sources=valid_data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config
        )

        assert plan.is_executable() is True

    def test_plan_not_executable_when_inactive(
            self, valid_data_sources, valid_output_config, valid_strategy_config
    ):
        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="TEST",
            active=False,
            data_sources=valid_data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config
        )

        assert plan.is_executable() is False

    def test_plan_not_executable_when_outside_validity_period(
            self, valid_data_sources, valid_output_config, valid_strategy_config
    ):
        validity = ValidityPeriod(
            valid_from=datetime(2020, 1, 1),
            valid_until=datetime(2020, 12, 31)
        )

        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="TEST",
            active=True,
            data_sources=valid_data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config,
            validity_period=validity
        )

        assert plan.is_executable() is False

    def test_plan_deactivate(self, valid_data_sources, valid_output_config, valid_strategy_config):
        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="TEST",
            active=True,
            data_sources=valid_data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config
        )

        plan.deactivate()

        assert plan.active is False

    def test_plan_activate(self, valid_data_sources, valid_output_config, valid_strategy_config):
        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="TEST",
            active=False,
            data_sources=valid_data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config
        )

        plan.activate()

        assert plan.active is True

    def test_plan_requires_multiple_sources(
            self, valid_output_config, valid_strategy_config
    ):
        source1 = DataSourceConfig("ventas", "s3", {})
        source2 = DataSourceConfig("empleados", "csv", {})

        from src.context.commissions.domain.value_objects import DataMergeConfig
        merge = DataMergeConfig("join", "ventas", {})
        data_sources = DataSourceCollection(sources=[source1, source2], merge_strategy=merge)

        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="TEST",
            active=True,
            data_sources=data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config
        )

        assert plan.requires_multiple_sources() is True

    def test_plan_single_source(
            self, valid_data_sources, valid_output_config, valid_strategy_config
    ):
        plan = Plan(
            id="PLAN_001",
            name="Test Plan",
            tenant_id="TEST",
            active=True,
            data_sources=valid_data_sources,
            output_config=valid_output_config,
            strategy_config=valid_strategy_config
        )

        assert plan.requires_multiple_sources() is False
