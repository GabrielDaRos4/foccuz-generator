import logging
from pathlib import Path

import yaml

from .plan_config import PlanConfig
from .tenant_config import TenantConfig

logger = logging.getLogger(__name__)


class PlanRegistry:
    def __init__(self, plans_directory: str = None):
        if plans_directory is None:
            project_root = Path(__file__).parent.parent.parent.parent.parent.parent
            plans_directory = project_root / 'config' / 'plans'

        self.plans_directory = Path(plans_directory)
        self.tenants: dict[str, TenantConfig] = {}
        self._load_all_clients()

    def _load_all_clients(self) -> None:
        if not self.plans_directory.exists():
            logger.warning(f"Plans directory does not exist: {self.plans_directory}")
            self.plans_directory.mkdir(parents=True, exist_ok=True)
            return

        yaml_files = list(self.plans_directory.glob('*.yaml')) + list(self.plans_directory.glob('*.yml'))

        for yaml_file in yaml_files:
            try:
                self._load_client_config(yaml_file)
            except Exception as e:
                logger.error(f"Error loading config from {yaml_file}: {str(e)}")

    def _load_client_config(self, config_path: Path) -> None:
        with open(config_path) as f:
            config = yaml.safe_load(f)

        tenant_config = TenantConfig(config_path, config)

        if not tenant_config.client_id:
            raise ValueError(f"Missing client_id in {config_path}")

        self.tenants[tenant_config.client_id] = tenant_config
        logger.info(f"Loaded tenant {tenant_config.client_id} with {len(tenant_config.plans)} plans")

    def get_tenant_config(self, tenant_id: str) -> TenantConfig | None:
        return self.tenants.get(tenant_id)

    def get_plan_config(self, full_plan_id: str) -> PlanConfig | None:
        parts = full_plan_id.split('.', 1)
        if len(parts) != 2:
            return None

        tenant_id, plan_id = parts
        tenant = self.get_tenant_config(tenant_id)

        if not tenant:
            return None

        return tenant.plans.get(plan_id)

    def list_all_tenants(self) -> list[str]:
        return list(self.tenants.keys())

    def list_plans_for_tenant(self, tenant_id: str) -> list[str]:
        tenant = self.get_tenant_config(tenant_id)
        if not tenant:
            return []
        return list(tenant.plans.keys())

    def list_all_plans(self) -> list[str]:
        plans = []
        for tenant in self.tenants.values():
            for plan_id in tenant.plans.keys():
                plans.append(f"{tenant.client_id}.{plan_id}")
        return plans

    def reload(self) -> None:
        self.tenants.clear()
        self._load_all_clients()
