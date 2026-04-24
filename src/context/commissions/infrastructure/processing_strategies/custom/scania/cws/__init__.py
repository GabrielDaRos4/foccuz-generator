from src.context.commissions.infrastructure.processing_strategies.custom.scania.cws.cws_manager_strategy import (
    CWSManagerStrategy,
    JefeCWSStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.cws.cws_supervisor_strategy import (
    CWSSupervisorStrategy,
    SupervisorCWSStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.cws.cws_technician_strategy import (
    CWSTechnicianStrategy,
    TecnicoCWSStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.cws.output_formatter import (
    CWSOutputFormatter,
)

__all__ = [
    "CWSManagerStrategy",
    "JefeCWSStrategy",
    "CWSSupervisorStrategy",
    "SupervisorCWSStrategy",
    "CWSTechnicianStrategy",
    "TecnicoCWSStrategy",
    "CWSOutputFormatter",
]
