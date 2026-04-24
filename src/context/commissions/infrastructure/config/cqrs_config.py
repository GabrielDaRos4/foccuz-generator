from src.context.commissions.application.commands import (
    ProcessAllTenantsCommand,
    ProcessAllTenantsHandler,
    ProcessPlanCommissionCommand,
    ProcessPlanCommissionHandler,
    ProcessTenantCommissionsCommand,
    ProcessTenantCommissionsHandler,
)
from src.context.commissions.application.queries import (
    GetActiveTenantsHandler,
    GetActiveTenantsQuery,
    GetTenantHandler,
    GetTenantQuery,
    ListTenantPlansHandler,
    ListTenantPlansQuery,
)
from src.context.commissions.domain.ports import Exporter, StrategyFactory
from src.context.commissions.domain.repositories import (
    MultiSourceDataRepository,
    TenantRepository,
)
from src.context.commissions.domain.services import CommissionCalculatorService
from src.context.shared.infrastructure.cqrs import CommandBus, QueryBus
from src.context.shared.infrastructure.di import DIContainer


def configure_command_bus(container: DIContainer) -> CommandBus:
    bus = CommandBus()

    plan_handler = ProcessPlanCommissionHandler(
        data_repo=container.resolve(MultiSourceDataRepository),
        calculator=container.resolve(CommissionCalculatorService),
        exporter=container.resolve(Exporter),
        strategy_factory=container.resolve(StrategyFactory)
    )

    tenant_handler = ProcessTenantCommissionsHandler(
        tenant_repo=container.resolve(TenantRepository),
        plan_handler=plan_handler,
        data_repo=container.resolve(MultiSourceDataRepository)
    )

    all_tenants_handler = ProcessAllTenantsHandler(
        tenant_repo=container.resolve(TenantRepository),
        tenant_handler=tenant_handler
    )

    bus.register(ProcessPlanCommissionCommand, plan_handler)
    bus.register(ProcessTenantCommissionsCommand, tenant_handler)
    bus.register(ProcessAllTenantsCommand, all_tenants_handler)

    return bus


def configure_query_bus(container: DIContainer) -> QueryBus:
    bus = QueryBus()

    tenant_repo = container.resolve(TenantRepository)

    bus.register(GetTenantQuery, GetTenantHandler(tenant_repo))
    bus.register(GetActiveTenantsQuery, GetActiveTenantsHandler(tenant_repo))
    bus.register(ListTenantPlansQuery, ListTenantPlansHandler(tenant_repo))

    return bus
