import pytest

from src.context.commissions.domain.aggregates import Plan, Tenant
from src.context.commissions.domain.exceptions import InvalidPlanError, InvalidTenantError
from src.context.commissions.domain.value_objects import (
    DataSourceCollection,
    DataSourceConfig,
    OutputConfig,
    StrategyConfig,
)


class TestTenant:

    def test_create_tenant(self):
        tenant = Tenant(
            id="test_tenant",
            name="Test Tenant",
            gsheet_id="sheet123"
        )

        assert tenant.id == "test_tenant"
        assert tenant.name == "Test Tenant"
        assert tenant.gsheet_id == "sheet123"
        assert tenant.active is True
        assert len(tenant.plans) == 0

    def test_tenant_requires_id(self):
        with pytest.raises(InvalidTenantError, match="Tenant ID cannot be empty"):
            Tenant(id="", name="Test", gsheet_id="sheet123")

    def test_tenant_requires_name(self):
        with pytest.raises(InvalidTenantError, match="Tenant name cannot be empty"):
            Tenant(id="test", name="", gsheet_id="sheet123")

    def test_tenant_requires_gsheet_id(self):
        with pytest.raises(InvalidTenantError, match="Google Sheet ID cannot be empty"):
            Tenant(id="test", name="Test", gsheet_id="")

    def test_add_plan(self):
        tenant = Tenant(id="test", name="Test", gsheet_id="sheet123")

        source = DataSourceConfig(source_id="default", source_type="json", config={"path": "test.json"})
        data_sources = DataSourceCollection(sources=[source])

        plan = Plan(
            id="plan1",
            name="Plan 1",
            tenant_id="test",
            active=True,
            data_sources=data_sources,
            output_config=OutputConfig(sheet_id="sheet123", tab_name="Tab1"),
            strategy_config=StrategyConfig(module="test", class_name="Test", params={})
        )

        tenant.add_plan(plan)

        assert len(tenant.plans) == 1
        assert tenant.plans[0] == plan

    def test_cannot_add_plan_from_different_tenant(self):
        tenant = Tenant(id="test1", name="Test 1", gsheet_id="sheet123")

        source = DataSourceConfig(source_id="default", source_type="json", config={"path": "test.json"})
        data_sources = DataSourceCollection(sources=[source])

        plan = Plan(
            id="plan1",
            name="Plan 1",
            tenant_id="test2",
            active=True,
            data_sources=data_sources,
            output_config=OutputConfig(sheet_id="sheet123", tab_name="Tab1"),
            strategy_config=StrategyConfig(module="test", class_name="Test", params={})
        )

        with pytest.raises(InvalidPlanError, match="Plan belongs to tenant test2"):
            tenant.add_plan(plan)

    def test_cannot_add_duplicate_plan(self):
        tenant = Tenant(id="test", name="Test", gsheet_id="sheet123")

        source = DataSourceConfig(source_id="default", source_type="json", config={"path": "test.json"})
        data_sources1 = DataSourceCollection(sources=[source])
        data_sources2 = DataSourceCollection(sources=[source])

        plan1 = Plan(
            id="plan1",
            name="Plan 1",
            tenant_id="test",
            active=True,
            data_sources=data_sources1,
            output_config=OutputConfig(sheet_id="sheet123", tab_name="Tab1"),
            strategy_config=StrategyConfig(module="test", class_name="Test", params={})
        )

        plan2 = Plan(
            id="plan1",
            name="Plan 1 Duplicate",
            tenant_id="test",
            active=True,
            data_sources=data_sources2,
            output_config=OutputConfig(sheet_id="sheet123", tab_name="Tab2"),
            strategy_config=StrategyConfig(module="test", class_name="Test", params={})
        )

        tenant.add_plan(plan1)

        with pytest.raises(InvalidPlanError, match="Plan plan1 already exists"):
            tenant.add_plan(plan2)

    def test_get_plan(self):
        tenant = Tenant(id="test", name="Test", gsheet_id="sheet123")

        source = DataSourceConfig(source_id="default", source_type="json", config={"path": "test.json"})
        data_sources = DataSourceCollection(sources=[source])

        plan = Plan(
            id="plan1",
            name="Plan 1",
            tenant_id="test",
            active=True,
            data_sources=data_sources,
            output_config=OutputConfig(sheet_id="sheet123", tab_name="Tab1"),
            strategy_config=StrategyConfig(module="test", class_name="Test", params={})
        )

        tenant.add_plan(plan)

        retrieved = tenant.get_plan("plan1")
        assert retrieved == plan

        not_found = tenant.get_plan("nonexistent")
        assert not_found is None

    def test_get_executable_plans(self):
        tenant = Tenant(id="test", name="Test", gsheet_id="sheet123")

        source = DataSourceConfig(source_id="default", source_type="json", config={"path": "test.json"})
        data_sources1 = DataSourceCollection(sources=[source])
        data_sources2 = DataSourceCollection(sources=[source])

        plan1 = Plan(
            id="plan1",
            name="Active Plan",
            tenant_id="test",
            active=True,
            data_sources=data_sources1,
            output_config=OutputConfig(sheet_id="sheet123", tab_name="Tab1"),
            strategy_config=StrategyConfig(module="test", class_name="Test", params={})
        )

        plan2 = Plan(
            id="plan2",
            name="Inactive Plan",
            tenant_id="test",
            active=False,
            data_sources=data_sources2,
            output_config=OutputConfig(sheet_id="sheet123", tab_name="Tab2"),
            strategy_config=StrategyConfig(module="test", class_name="Test", params={})
        )

        tenant.add_plan(plan1)
        tenant.add_plan(plan2)

        executable = tenant.get_executable_plans()

        assert len(executable) == 1
        assert executable[0] == plan1

    def test_activate_tenant(self):
        tenant = Tenant(id="test", name="Test", gsheet_id="sheet123", active=False)

        tenant.activate()

        assert tenant.active is True

    def test_deactivate_tenant(self):
        tenant = Tenant(id="test", name="Test", gsheet_id="sheet123")

        source = DataSourceConfig(source_id="default", source_type="json", config={"path": "test.json"})
        data_sources = DataSourceCollection(sources=[source])

        plan = Plan(
            id="plan1",
            name="Plan 1",
            tenant_id="test",
            active=True,
            data_sources=data_sources,
            output_config=OutputConfig(sheet_id="sheet123", tab_name="Tab1"),
            strategy_config=StrategyConfig(module="test", class_name="Test", params={})
        )

        tenant.add_plan(plan)
        tenant.deactivate()

        assert tenant.active is False
        assert plan.active is False
        assert len(tenant.get_executable_plans()) == 0
