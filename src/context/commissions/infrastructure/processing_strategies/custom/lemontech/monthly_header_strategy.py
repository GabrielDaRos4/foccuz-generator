import logging

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .monthly_commission_strategy import COMPLIANCE_TIERS

logger = logging.getLogger(__name__)


class LemontechMonthlyHeaderStrategy(ProcessingStrategy):

    def __init__(
        self,
        target_period: str = None,
        rep_id_filter: str = None,
    ):
        self._target_period = target_period
        self._rep_id_filter = rep_id_filter

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        goals_df = data.attrs.get('goals')

        if goals_df is None or goals_df.empty:
            logger.warning("No goals data found")
            return pd.DataFrame()

        deals = self._prepare_deals(data)

        if deals.empty:
            logger.warning("No valid deals after preparation")
            return pd.DataFrame()

        goals = self._prepare_goals(goals_df)

        header = self._calculate_header(deals, goals)

        if header.empty:
            return pd.DataFrame()

        if self._rep_id_filter:
            header = header[header['Rep ID'] == str(self._rep_id_filter)]

        result = self._format_output(header)

        return result

    @staticmethod
    def _prepare_deals(data: pd.DataFrame) -> pd.DataFrame:
        deals = data.copy()

        deals['closeDate'] = pd.to_datetime(deals['closeDate']) - pd.Timedelta(hours=4)
        deals = deals[deals['closeDate'] >= '2025-01-01']

        deals['Amount in company currency'] = pd.to_numeric(
            deals['Amount in company currency'], errors='coerce'
        ).fillna(0)

        deals = deals[deals['ownerRepId'].notna()]
        deals['ownerRepId'] = deals['ownerRepId'].astype(int).astype(str)

        deals['Fecha'] = deals['closeDate'].dt.to_period('M').dt.to_timestamp()

        return deals

    @staticmethod
    def _prepare_goals(goals_df: pd.DataFrame) -> pd.DataFrame:
        goals = goals_df.copy()
        goals.columns = goals.columns.str.strip()

        if 'Rep ID' in goals.columns:
            goals['Rep ID'] = goals['Rep ID'].astype(str).str.strip()

        if 'Fecha' in goals.columns:
            goals['Fecha'] = pd.to_datetime(goals['Fecha'], errors='coerce')

        if 'Meta' in goals.columns:
            goals['Meta'] = pd.to_numeric(goals['Meta'], errors='coerce').fillna(0)

        return goals

    @staticmethod
    def _calculate_header(
        deals: pd.DataFrame,
        goals: pd.DataFrame
    ) -> pd.DataFrame:
        monthly_totals = deals.groupby(['ownerRepId', 'Fecha'], as_index=False).agg({
            'Amount in company currency': 'sum'
        })

        monthly_totals['ownerRepId'] = monthly_totals['ownerRepId'].astype(str)

        header = pd.merge(
            monthly_totals,
            goals[['Rep ID', 'Fecha', 'Meta']],
            left_on=['ownerRepId', 'Fecha'],
            right_on=['Rep ID', 'Fecha'],
            how='left'
        )

        header = header.drop(columns=['ownerRepId'], errors='ignore')
        header = header[header['Meta'].notna() & (header['Meta'] > 0)]

        header['Cumplimiento'] = header['Amount in company currency'] / header['Meta']

        def get_rate(compliance):
            for threshold, rate in COMPLIANCE_TIERS:
                if compliance >= threshold:
                    return rate
            return 0

        header['% Comision'] = header['Cumplimiento'].apply(get_rate)

        header['Subtotal'] = header['Amount in company currency'] * header['% Comision']

        return header

    @staticmethod
    def _format_output(header: pd.DataFrame) -> pd.DataFrame:
        if header.empty:
            return pd.DataFrame()

        header = header.copy()

        header['Fecha'] = header['Fecha'].dt.strftime('%Y-%m-%d')

        output_columns = [
            'Rep ID',
            'Fecha',
            'Amount in company currency',
            'Meta',
            'Cumplimiento',
            '% Comision',
            'Subtotal',
        ]

        existing_cols = [c for c in output_columns if c in header.columns]
        result = header[existing_cols].copy()

        result.attrs['column_types'] = {
            'Rep ID': 'text',
            'Fecha': 'text',
            'Amount in company currency': 'money',
            'Meta': 'money',
            'Cumplimiento': 'percent',
            '% Comision': 'percent',
            'Subtotal': 'money',
        }

        return result
