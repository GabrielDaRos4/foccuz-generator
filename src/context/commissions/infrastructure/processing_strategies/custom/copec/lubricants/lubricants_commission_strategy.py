import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .output_formatter import LubricantsOutputFormatter

logger = logging.getLogger(__name__)


class CopecLubricantsCommissionStrategy(ProcessingStrategy):

    COLUMN_MAPPINGS = {
        'rut': 'rut',
        'vendedor': 'vendedor',
        'cliente': 'cliente',
        'solicitante': 'solicitante',
        'vol. 2025': 'volumen',
        'desc-pl 2025': 'descuento',
        'comision $': 'commission',
    }

    def __init__(
        self,
        target_period: str = None,
        rep_id_filter: str = None
    ):
        self._output_formatter = LubricantsOutputFormatter()
        self._rep_id_filter = rep_id_filter
        self._target_period = target_period

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        data = self._normalize_columns(data)
        data = self._map_columns(data)

        data = self._filter_valid_rows(data)
        if data.empty:
            return pd.DataFrame()

        data = self._build_rep_id(data)

        if self._rep_id_filter:
            data = self._filter_by_rep_id(data)
            if data.empty:
                return pd.DataFrame()

        data = self._calculate_commission_per_liter(data)

        period = self._extract_period()

        return self._output_formatter.format(data, period)

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()
        return df

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_dict = {}
        for original, target in self.COLUMN_MAPPINGS.items():
            if original in df.columns:
                rename_dict[original] = target
        return df.rename(columns=rename_dict)

    @staticmethod
    def _filter_valid_rows(df: pd.DataFrame) -> pd.DataFrame:
        if 'solicitante' not in df.columns:
            logger.warning("Column 'solicitante' not found")
            return pd.DataFrame()

        df = df[df['solicitante'].notna()].copy()
        df = df[df['solicitante'].astype(str).str.strip() != ''].copy()

        if 'volumen' in df.columns:
            df['volumen'] = pd.to_numeric(df['volumen'], errors='coerce').fillna(0)
            df = df[df['volumen'] > 0].copy()

        logger.info(f"Filtered to {len(df)} valid rows")
        return df

    @staticmethod
    def _build_rep_id(df: pd.DataFrame) -> pd.DataFrame:
        if 'rut' in df.columns:
            df['rep_id'] = (
                df['rut'].astype(str)
                .str.strip()
                .str.replace('.', '', regex=False)
                .str.upper()
            )
        else:
            df['rep_id'] = ''
        return df

    def _filter_by_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_id_str = str(self._rep_id_filter).strip()
        filtered = df[df["rep_id"].astype(str).str.strip() == rep_id_str].copy()
        logger.info(f"Filtered by Rep ID {rep_id_str}: {len(filtered)} records")
        return filtered

    @staticmethod
    def _calculate_commission_per_liter(df: pd.DataFrame) -> pd.DataFrame:
        if 'commission' in df.columns and 'volumen' in df.columns:
            df['commission'] = pd.to_numeric(df['commission'], errors='coerce').fillna(0)
            df['volumen'] = pd.to_numeric(df['volumen'], errors='coerce').fillna(0)

            df['commission_per_liter'] = df.apply(
                lambda row: row['commission'] / row['volumen'] if row['volumen'] > 0 else 0,
                axis=1
            )
        else:
            df['commission_per_liter'] = 0

        return df

    def _extract_period(self) -> datetime:
        if self._target_period:
            try:
                if isinstance(self._target_period, datetime):
                    return self._target_period
                if hasattr(self._target_period, 'year'):
                    return datetime(
                        self._target_period.year,
                        self._target_period.month,
                        self._target_period.day
                    )
                return datetime.strptime(str(self._target_period), '%Y-%m-%d')
            except (ValueError, AttributeError) as e:
                logger.error(f"Error parsing target_period: {e}")
        return datetime.now().replace(day=1)

    def get_column_types(self) -> dict[str, str]:
        return self._output_formatter.COLUMN_TYPES
