import logging

from src.context.commissions.domain.aggregates import Plan, Tenant
from src.context.commissions.domain.repositories import TenantRepository
from src.context.commissions.infrastructure.config.plan_registry import PlanRegistry

logger = logging.getLogger(__name__)


class YAMLTenantRepository(TenantRepository):
    def __init__(self, registry: PlanRegistry):
        self.registry = registry

    def get_by_id(self, tenant_id: str) -> Tenant | None:
        tenant_config = self.registry.get_tenant_config(tenant_id)

        if not tenant_config:
            logger.warning(f"Tenant {tenant_id} not found in registry")
            return None

        tenant = Tenant(
            id=tenant_config.client_id,
            name=tenant_config.client_name,
            gsheet_id=tenant_config.gsheet_output,
            plans=[],
            active=tenant_config.active
        )

        for plan_id, plan_config in tenant_config.plans.items():
            plan = Plan(
                id=plan_id,
                name=plan_config.name,
                tenant_id=tenant_config.client_id,
                active=plan_config.active,
                data_sources=plan_config.data_source_config,
                output_config=plan_config.output_config,
                strategy_config=plan_config.strategy_config,
                validity_period=plan_config.validity_period,
                depends_on=plan_config.depends_on
            )
            tenant.add_plan(plan)

        logger.info(f"Loaded tenant {tenant_id} with {len(tenant.plans)} plans")
        return tenant

    def get_all(self) -> list[Tenant]:
        tenants = []
        for tenant_id in self.registry.list_all_tenants():
            tenant = self.get_by_id(tenant_id)
            if tenant:
                tenants.append(tenant)
        return tenants

    def get_active_tenants(self) -> list[Tenant]:
        all_tenants = self.get_all()
        return [t for t in all_tenants if t.active]

