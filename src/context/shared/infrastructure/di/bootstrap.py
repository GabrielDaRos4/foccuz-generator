from src.context.commissions.domain.ports import (
    Exporter,
    MergeStrategyRegistry,
    StrategyFactory,
)
from src.context.commissions.domain.repositories import (
    MultiSourceDataRepository,
    TenantRepository,
)
from src.context.commissions.domain.services import CommissionCalculatorService
from src.context.commissions.infrastructure.config.plan_registry import PlanRegistry
from src.context.commissions.infrastructure.config.strategy_factory import (
    DynamicStrategyFactory,
)
from src.context.commissions.infrastructure.exporters.gsheet_exporter import (
    GSheetExporter,
)
from src.context.commissions.infrastructure.repositories.api_data_repository import (
    APIDataRepository,
)
from src.context.commissions.infrastructure.repositories.buk_data_repository import (
    BuKDataRepository,
)
from src.context.commissions.infrastructure.repositories.composite_multi_source_repository import (
    CompositeMultiSourceDataRepository,
)
from src.context.commissions.infrastructure.repositories.csv_data_repository import (
    CSVDataRepository,
)
from src.context.commissions.infrastructure.repositories.csv_pattern_data_repository import (
    CSVPatternDataRepository,
)
from src.context.commissions.infrastructure.repositories.excel_data_repository import (
    ExcelDataRepository,
)
from src.context.commissions.infrastructure.repositories.foccuz_data_repository import (
    FoccuzDataRepository,
)
from src.context.commissions.infrastructure.repositories.gsheet_data_repository import (
    GSheetDataRepository,
)
from src.context.commissions.infrastructure.repositories.json_data_repository import (
    JSONDataRepository,
)
from src.context.commissions.infrastructure.repositories.s3_data_repository import (
    S3DataRepository,
)
from src.context.commissions.infrastructure.repositories.yaml_tenant_repository import (
    YAMLTenantRepository,
)
from src.context.commissions.infrastructure.services import InMemoryMergeStrategyRegistry
from src.context.shared.infrastructure.cqrs import CommandBus, QueryBus

from .di_container import DIContainer


def _register_merge_strategies(registry: MergeStrategyRegistry) -> None:
    from src.context.commissions.infrastructure.processing_strategies.custom.copec import (
        copec_lubricants_merge,
        copec_new_client_merge,
        copec_poa_compliance_merge,
    )
    from src.context.commissions.infrastructure.processing_strategies.custom.copec.quarterly_team import (
        quarterly_team_merge as _qtm_module,
    )
    copec_quarterly_team_merge = _qtm_module.copec_quarterly_team_merge
    from src.context.commissions.infrastructure.processing_strategies.custom.copec.summary.summary_merge import (
        copec_summary_merge,
    )
    from src.context.commissions.infrastructure.processing_strategies.custom.gocar import (
        gocar_commission_merge,
    )
    from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
        grupo_vanguardia_sales_merge,
        monedero_sales_merge,
    )
    from src.context.commissions.infrastructure.processing_strategies.custom.grupok import (
        grupok_product_manager_merge,
        grupok_sales_advisor_merge,
        grupok_store_manager_merge,
        grupok_subgerente_merge,
    )
    from src.context.commissions.infrastructure.processing_strategies.custom.lemontech import (
        lemontech_monthly_merge,
        lemontech_quarterly_merge,
    )
    from src.context.commissions.infrastructure.processing_strategies.custom.scania.scania_merge import (
        scania_generic_merge,
    )

    registry.register('scania_generic_merge', scania_generic_merge)
    registry.register('copec_new_client_merge', copec_new_client_merge)
    registry.register('copec_tct_merge', copec_new_client_merge)
    registry.register('copec_lubricants_merge', copec_lubricants_merge)
    registry.register('copec_poa_compliance_merge', copec_poa_compliance_merge)
    registry.register('copec_summary_merge', copec_summary_merge)
    registry.register('copec_quarterly_team_merge', copec_quarterly_team_merge)
    registry.register('grupo_vanguardia_sales_merge', grupo_vanguardia_sales_merge)
    registry.register('monedero_sales_merge', monedero_sales_merge)
    registry.register('gocar_commission_merge', gocar_commission_merge)
    registry.register('grupok_sales_advisor_merge', grupok_sales_advisor_merge)
    registry.register('grupok_store_manager_merge', grupok_store_manager_merge)
    registry.register('grupok_product_manager_merge', grupok_product_manager_merge)
    registry.register('grupok_subgerente_merge', grupok_subgerente_merge)
    registry.register('lemontech_monthly_merge', lemontech_monthly_merge)
    registry.register('lemontech_quarterly_merge', lemontech_quarterly_merge)


def _create_source_repositories(credentials_path: str = None) -> dict:
    return {
        's3': S3DataRepository(),
        'json': JSONDataRepository(),
        'buk': BuKDataRepository(),
        'csv': CSVDataRepository(),
        'csv_pattern': CSVPatternDataRepository(),
        'api': APIDataRepository(),
        'excel': ExcelDataRepository(),
        'foccuz': FoccuzDataRepository(),
        'gsheet': GSheetDataRepository(credentials_path=credentials_path),
    }


def _create_merge_registry() -> MergeStrategyRegistry:
    registry = InMemoryMergeStrategyRegistry()
    _register_merge_strategies(registry)
    return registry


def bootstrap(plans_directory: str = None, credentials_path: str = None) -> DIContainer:
    container = DIContainer()

    container.register_singleton(
        PlanRegistry,
        lambda: PlanRegistry(plans_directory=plans_directory)
    )

    container.register_singleton(
        MergeStrategyRegistry,
        lambda: _create_merge_registry()
    )

    container.register_singleton(
        TenantRepository,
        lambda: YAMLTenantRepository(registry=container.resolve(PlanRegistry))
    )

    container.register_singleton(
        MultiSourceDataRepository,
        lambda: CompositeMultiSourceDataRepository(
            source_repositories=_create_source_repositories(credentials_path=credentials_path),
            merge_registry=container.resolve(MergeStrategyRegistry)
        )
    )

    container.register_singleton(
        CommissionCalculatorService,
        lambda: CommissionCalculatorService()
    )

    container.register_singleton(
        Exporter,
        lambda: GSheetExporter(credentials_path=credentials_path)
    )

    container.register_singleton(
        StrategyFactory,
        lambda: DynamicStrategyFactory()
    )

    from src.context.commissions.infrastructure.config.cqrs_config import (
        configure_command_bus,
        configure_query_bus,
    )

    container.register_singleton(
        CommandBus,
        lambda: configure_command_bus(container)
    )

    container.register_singleton(
        QueryBus,
        lambda: configure_query_bus(container)
    )

    return container
