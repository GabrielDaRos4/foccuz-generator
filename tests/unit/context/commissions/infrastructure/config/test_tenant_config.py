from pathlib import Path

import pytest

from src.context.commissions.infrastructure.config import (
    TenantConfig,
)


class TestTenantConfig:

    @pytest.fixture
    def sample_config(self):
        return {
            "metadata": {
                "client_id": "TENANT1",
                "client_name": "Test Tenant",
                "gsheet_output": "sheet-id",
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

    def test_creates_tenant_config(self, sample_config):
        tenant = TenantConfig(Path("/test/path.yaml"), sample_config)

        assert tenant.client_id == "TENANT1"
        assert tenant.client_name == "Test Tenant"
        assert tenant.active is True

    def test_loads_plans(self, sample_config):
        tenant = TenantConfig(Path("/test/path.yaml"), sample_config)

        assert "PLAN_001" in tenant.plans
        assert tenant.plans["PLAN_001"].name == "Test Plan"

    def test_stores_config_path(self, sample_config):
        path = Path("/test/path.yaml")
        tenant = TenantConfig(path, sample_config)

        assert tenant.config_path == path

    def test_defaults_active_to_false(self):
        config = {
            "metadata": {"client_id": "TEST"},
            "plans": {}
        }
        tenant = TenantConfig(Path("/test.yaml"), config)

        assert tenant.active is False
