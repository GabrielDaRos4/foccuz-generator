import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from ..shared import ProductFilter, RutBuilder
from .client_classifier import ClientClassifier
from .commission_calculator import CommissionCalculator
from .commission_config import CommissionConfig
from .historical_client_analyzer import HistoricalClientAnalyzer
from .output_formatter import CopecOutputFormatter, extract_period

logger = logging.getLogger(__name__)


class CopecNewClientCommissionStrategy(ProcessingStrategy):

    def __init__(
        self,
        product_type: str | list[str],
        discount_percentage: float,
        max_factor: float,
        bono_nuevo: float = 0,
        factor_minimo: float = 0.5,
        months_lookback: int = 14,
        target_period: str = None,
        rep_id_filter: str = None,
        volume_col: str = None,
        discount_col: str = None
    ):
        self._product_filter = ProductFilter(product_type, volume_col, discount_col)
        self._rut_builder = RutBuilder()
        self._historical_analyzer = HistoricalClientAnalyzer(product_type, months_lookback)
        self._client_classifier = ClientClassifier()
        self._commission_calculator = CommissionCalculator(
            CommissionConfig(discount_percentage, max_factor, bono_nuevo, factor_minimo)
        )
        include_bonus_column = bono_nuevo > 0
        self._output_formatter = CopecOutputFormatter(include_bonus_column=include_bonus_column)
        self._rep_id_filter = rep_id_filter
        self._target_period = target_period

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        historical = data.attrs.get("ventas_historicas", [])
        data = self._normalize_columns(data)

        filtered = self._product_filter.filter(data)
        if filtered.empty:
            return pd.DataFrame()

        filtered = self._product_filter.filter_positive_volume(filtered)
        if filtered.empty:
            return pd.DataFrame()

        filtered = self._rut_builder.build(filtered)

        if self._rep_id_filter:
            filtered = self._filter_by_rep_id(filtered)
            if filtered.empty:
                return pd.DataFrame()

        clients_m1, clients_m2, clients_hist = self._historical_analyzer.analyze(historical)

        new_clients = self._client_classifier.classify(
            filtered, clients_m1, clients_m2, clients_hist
        )
        if new_clients.empty:
            return pd.DataFrame()

        result = self._commission_calculator.calculate(new_clients)
        period = self._get_period(result)

        return self._output_formatter.format(result, period)

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()
        return df

    def _filter_by_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_id_str = str(self._rep_id_filter).zfill(10)
        filtered = df[df["ejecutivo"].astype(str).str.zfill(10) == rep_id_str].copy()
        logger.info(f"Filtered by Rep ID {rep_id_str}: {len(filtered)} records")
        return filtered

    def get_column_types(self) -> dict[str, str]:
        return self._output_formatter.COLUMN_TYPES

    def _get_period(self, df: pd.DataFrame) -> datetime:
        if self._target_period:
            return datetime.strptime(str(self._target_period), '%Y-%m-%d')
        return extract_period(df)
