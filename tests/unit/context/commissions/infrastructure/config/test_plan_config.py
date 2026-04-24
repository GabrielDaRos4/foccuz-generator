import pytest

from src.context.commissions.infrastructure.config import (
    PlanConfig,
)


class TestPlanConfig:

    @pytest.fixture
    def sample_config(self):
        return {
            "name": "Test Plan",
            "active": True,
            "script": {
                "module": "src.module",
                "class": "TestStrategy",
                "params": {"param1": "value1"}
            },
            "data_source": {
                "type": "csv",
                "config": {"path": "/test/path"}
            },
            "output": {
                "sheet_id": "test-sheet-id",
                "tab_name": "TestTab",
                "clear_before_write": True
            }
        }

    def test_creates_plan_config(self, sample_config):
        plan = PlanConfig("TENANT1", "PLAN_001", sample_config)

        assert plan.tenant_id == "TENANT1"
        assert plan.plan_id == "PLAN_001"
        assert plan.name == "Test Plan"
        assert plan.active is True

    def test_full_id_property(self, sample_config):
        plan = PlanConfig("TENANT1", "PLAN_001", sample_config)

        assert plan.full_id == "TENANT1.PLAN_001"

    def test_creates_strategy_config(self, sample_config):
        plan = PlanConfig("TENANT1", "PLAN_001", sample_config)

        assert plan.strategy_config.module == "src.module"
        assert plan.strategy_config.class_name == "TestStrategy"
        assert plan.strategy_config.params["param1"] == "value1"

    def test_creates_output_config(self, sample_config):
        plan = PlanConfig("TENANT1", "PLAN_001", sample_config)

        assert plan.output_config.sheet_id == "test-sheet-id"
        assert plan.output_config.tab_name == "TestTab"

    def test_defaults_active_to_true(self):
        config = {
            "name": "Test",
            "script": {
                "module": "src.module",
                "class": "TestStrategy"
            },
            "data_source": {"type": "csv", "config": {}},
            "output": {
                "sheet_id": "test-sheet-id",
                "tab_name": "TestTab"
            }
        }
        plan = PlanConfig("TENANT1", "PLAN_001", config)

        assert plan.active is True
