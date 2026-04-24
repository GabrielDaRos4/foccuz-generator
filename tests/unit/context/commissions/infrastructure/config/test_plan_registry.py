import tempfile
from pathlib import Path

import pytest
import yaml

from src.context.commissions.infrastructure.config import (
    PlanRegistry,
)


class TestPlanRegistry:

    @pytest.fixture
    def temp_plans_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sample_tenant_config(self):
        return {
            "metadata": {
                "client_id": "TENANT1",
                "client_name": "Test Tenant",
                "active": True
            },
            "plans": {
                "PLAN_001": {
                    "name": "Test Plan",
                    "active": True,
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
            }
        }

    def test_creates_registry_with_empty_directory(self, temp_plans_dir):
        registry = PlanRegistry(str(temp_plans_dir))

        assert registry.plans_directory == temp_plans_dir
        assert len(registry.tenants) == 0

    def test_loads_yaml_config(self, temp_plans_dir, sample_tenant_config):
        config_file = temp_plans_dir / "tenant1.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_tenant_config, f)

        registry = PlanRegistry(str(temp_plans_dir))

        assert "TENANT1" in registry.tenants
        assert registry.tenants["TENANT1"].client_name == "Test Tenant"

    def test_loads_yml_config(self, temp_plans_dir, sample_tenant_config):
        config_file = temp_plans_dir / "tenant1.yml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_tenant_config, f)

        registry = PlanRegistry(str(temp_plans_dir))

        assert "TENANT1" in registry.tenants

    def test_get_tenant_config(self, temp_plans_dir, sample_tenant_config):
        config_file = temp_plans_dir / "tenant1.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_tenant_config, f)

        registry = PlanRegistry(str(temp_plans_dir))
        tenant = registry.get_tenant_config("TENANT1")

        assert tenant is not None
        assert tenant.client_id == "TENANT1"

    def test_get_tenant_config_returns_none_for_unknown(self, temp_plans_dir):
        registry = PlanRegistry(str(temp_plans_dir))

        assert registry.get_tenant_config("UNKNOWN") is None

    def test_get_plan_config(self, temp_plans_dir, sample_tenant_config):
        config_file = temp_plans_dir / "tenant1.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_tenant_config, f)

        registry = PlanRegistry(str(temp_plans_dir))
        plan = registry.get_plan_config("TENANT1.PLAN_001")

        assert plan is not None
        assert plan.plan_id == "PLAN_001"
        assert plan.name == "Test Plan"

    def test_get_plan_config_returns_none_for_invalid_format(self, temp_plans_dir):
        registry = PlanRegistry(str(temp_plans_dir))

        assert registry.get_plan_config("INVALID") is None

    def test_get_plan_config_returns_none_for_unknown_tenant(self, temp_plans_dir):
        registry = PlanRegistry(str(temp_plans_dir))

        assert registry.get_plan_config("UNKNOWN.PLAN_001") is None

    def test_list_all_tenants(self, temp_plans_dir, sample_tenant_config):
        config_file = temp_plans_dir / "tenant1.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_tenant_config, f)

        tenant2_config = sample_tenant_config.copy()
        tenant2_config["metadata"] = {"client_id": "TENANT2", "client_name": "Tenant 2"}
        config_file2 = temp_plans_dir / "tenant2.yaml"
        with open(config_file2, 'w') as f:
            yaml.dump(tenant2_config, f)

        registry = PlanRegistry(str(temp_plans_dir))
        tenants = registry.list_all_tenants()

        assert "TENANT1" in tenants
        assert "TENANT2" in tenants

    def test_list_plans_for_tenant(self, temp_plans_dir, sample_tenant_config):
        config_file = temp_plans_dir / "tenant1.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_tenant_config, f)

        registry = PlanRegistry(str(temp_plans_dir))
        plans = registry.list_plans_for_tenant("TENANT1")

        assert "PLAN_001" in plans

    def test_list_plans_for_unknown_tenant(self, temp_plans_dir):
        registry = PlanRegistry(str(temp_plans_dir))

        assert registry.list_plans_for_tenant("UNKNOWN") == []

    def test_list_all_plans(self, temp_plans_dir, sample_tenant_config):
        config_file = temp_plans_dir / "tenant1.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_tenant_config, f)

        registry = PlanRegistry(str(temp_plans_dir))
        plans = registry.list_all_plans()

        assert "TENANT1.PLAN_001" in plans

    def test_reload_clears_and_reloads(self, temp_plans_dir, sample_tenant_config):
        config_file = temp_plans_dir / "tenant1.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(sample_tenant_config, f)

        registry = PlanRegistry(str(temp_plans_dir))
        assert "TENANT1" in registry.tenants

        # Remove the file and reload
        config_file.unlink()
        registry.reload()

        assert "TENANT1" not in registry.tenants

    def test_creates_directory_if_not_exists(self, temp_plans_dir):
        non_existent = temp_plans_dir / "non_existent"
        registry = PlanRegistry(str(non_existent))

        assert non_existent.exists()
        assert len(registry.tenants) == 0
