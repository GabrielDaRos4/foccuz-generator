import logging

import pandas as pd

from ..shared import RutBuilder

logger = logging.getLogger(__name__)


class HistoricalClientAnalyzer:

    def __init__(self, product_type: str | list[str], months_lookback: int = 14):
        if isinstance(product_type, list):
            self._product_type = [p.upper() for p in product_type]
        else:
            self._product_type = product_type.upper()
        self._months_lookback = months_lookback
        self._rut_builder = RutBuilder()

    def analyze(self, historical_sales: list[pd.DataFrame]) -> tuple[set, set, set]:
        logger.info(f"Historical datasets available: {len(historical_sales)}")

        clients_m1 = self._extract_clients(historical_sales, 0)
        clients_m2 = self._extract_clients(historical_sales, 1)
        clients_historical = self._extract_historical_clients(historical_sales)

        return clients_m1, clients_m2, clients_historical

    def _extract_clients(self, historical: list, index: int) -> set[str]:
        if len(historical) <= index:
            return set()

        clients = self._rut_builder.extract_client_ruts(historical[index], self._product_type)
        logger.info(f"M-{index + 1}: {len(clients)} unique clients")
        return clients

    def _extract_historical_clients(self, historical: list) -> set[str]:
        if len(historical) < 3:
            return set()

        clients = set()
        for i in range(2, min(len(historical), self._months_lookback)):
            clients.update(self._rut_builder.extract_client_ruts(historical[i], self._product_type))

        logger.info(f"M-3 onwards: {len(clients)} unique clients")
        return clients
