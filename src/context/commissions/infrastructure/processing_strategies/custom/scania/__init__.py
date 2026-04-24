from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.claims import (
    ClaimsAdvisorStrategy,
    ClaimsWarehouseOperatorStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.compliance import (
    BusSalesExecutiveStrategy,
    CDAdminStrategy,
    CDOperatorStrategy,
    CommercialManagerStrategy,
    ControlTowerStrategy,
    EngineCoordinatorStrategy,
    GenericComplianceStrategy,
    MarketCoordinatorStrategy,
    NewSalesExecutiveStrategy,
    PartsSalesRepStrategy,
    PresalesStrategy,
    RegionsOperatorStrategy,
    SantiagoOperatorStrategy,
    ServicesCommercialAdvisorStrategy,
    TechnicalAssistanceStrategy,
    UsedSalesExecutiveStrategy,
    ZoneManagerStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.cws import (
    CWSManagerStrategy,
    CWSSupervisorStrategy,
    CWSTechnicianStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.sales import (
    SalesCommissionStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.scania_merge import (
    scania_generic_merge,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.service_manager import (
    AdminManagerStrategy,
    DesaboManagerStrategy,
    RBManagerStrategy,
    ServiceAdvisorStrategy,
    ServiceManagerStrategy,
    WorkshopManagerStrategy,
    WorkshopSupervisorStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.technician import (
    ClaimsTechnicianStrategy,
    MechanicTechnicianStrategy,
)

__all__ = [
    "BaseScaniaStrategy",
    "scania_generic_merge",
    "ServiceManagerStrategy",
    "WorkshopManagerStrategy",
    "WorkshopSupervisorStrategy",
    "ServiceAdvisorStrategy",
    "RBManagerStrategy",
    "AdminManagerStrategy",
    "DesaboManagerStrategy",
    "GenericComplianceStrategy",
    "CDOperatorStrategy",
    "CDAdminStrategy",
    "PartsSalesRepStrategy",
    "ZoneManagerStrategy",
    "EngineCoordinatorStrategy",
    "PresalesStrategy",
    "ControlTowerStrategy",
    "SantiagoOperatorStrategy",
    "RegionsOperatorStrategy",
    "MarketCoordinatorStrategy",
    "CommercialManagerStrategy",
    "NewSalesExecutiveStrategy",
    "UsedSalesExecutiveStrategy",
    "BusSalesExecutiveStrategy",
    "ServicesCommercialAdvisorStrategy",
    "TechnicalAssistanceStrategy",
    "CWSManagerStrategy",
    "CWSSupervisorStrategy",
    "CWSTechnicianStrategy",
    "MechanicTechnicianStrategy",
    "ClaimsTechnicianStrategy",
    "ClaimsWarehouseOperatorStrategy",
    "ClaimsAdvisorStrategy",
    "SalesCommissionStrategy",
]
