import logging

import numpy as np
import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

logger = logging.getLogger(__name__)

MIN_MONTHLY_COMPLIANCE = 0.6
QUARTERLY_BONUS_RATE = 0.2


class LemontechQuarterlyHeaderStrategy(ProcessingStrategy):

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

        monthly_compliance = self._calculate_monthly_compliance(deals, goals)
        quarterly_eligibility = self._calculate_quarterly_eligibility(monthly_compliance)

        header = self._calculate_quarterly_header(deals, goals)

        if header.empty:
            return pd.DataFrame()

        header = self._merge_with_eligibility(header, quarterly_eligibility)
        header = self._calculate_quarterly_bonus(header)

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
        deals['Quarter'] = deals['closeDate'].dt.to_period('Q').dt.end_time

        return deals

    @staticmethod
    def _prepare_goals(goals_df: pd.DataFrame) -> pd.DataFrame:
        goals = goals_df.copy()
        goals.columns = goals.columns.str.strip()

        if 'Rep ID' in goals.columns:
            goals['Rep ID'] = goals['Rep ID'].astype(str).str.strip()

        if 'Fecha' in goals.columns:
            goals['Fecha'] = pd.to_datetime(goals['Fecha'], errors='coerce')
            goals['Quarter'] = goals['Fecha'].dt.to_period('Q').dt.end_time

        if 'Meta' in goals.columns:
            goals['Meta'] = pd.to_numeric(goals['Meta'], errors='coerce').fillna(0)

        return goals

    @staticmethod
    def _calculate_monthly_compliance(
        deals: pd.DataFrame,
        goals: pd.DataFrame
    ) -> pd.DataFrame:
        monthly_totals = deals.groupby(['ownerRepId', 'Fecha'], as_index=False).agg({
            'Amount in company currency': 'sum'
        })

        monthly_totals['ownerRepId'] = monthly_totals['ownerRepId'].astype(str)

        merged = pd.merge(
            monthly_totals,
            goals[['Rep ID', 'Fecha', 'Meta']],
            left_on=['ownerRepId', 'Fecha'],
            right_on=['Rep ID', 'Fecha'],
            how='left'
        )

        merged = merged.drop(columns=['Rep ID'], errors='ignore')
        merged = merged[merged['Meta'].notna() & (merged['Meta'] > 0)]

        merged['compliance'] = merged['Amount in company currency'] / merged['Meta']
        merged['met_minimum'] = (merged['compliance'] >= MIN_MONTHLY_COMPLIANCE).astype(int)

        merged['Quarter'] = merged['Fecha'].dt.to_period('Q').dt.end_time

        return merged

    @staticmethod
    def _calculate_quarterly_eligibility(
        monthly_compliance: pd.DataFrame
    ) -> pd.DataFrame:
        quarterly_check = monthly_compliance.groupby(
            ['ownerRepId', 'Quarter'],
            as_index=False
        ).agg({
            'met_minimum': 'sum'
        })

        quarterly_check['Cumplimiento Todos los Meses'] = quarterly_check['met_minimum'] == 3

        return quarterly_check[['ownerRepId', 'Quarter', 'Cumplimiento Todos los Meses']]

    @staticmethod
    def _calculate_quarterly_header(
        deals: pd.DataFrame,
        goals: pd.DataFrame
    ) -> pd.DataFrame:
        quarterly_totals = deals.groupby(['ownerRepId', 'Quarter'], as_index=False).agg({
            'Amount in company currency': 'sum'
        })

        quarterly_goals = goals.groupby(['Rep ID', 'Quarter'], as_index=False).agg({
            'Meta': 'sum'
        })

        quarterly_totals['ownerRepId'] = quarterly_totals['ownerRepId'].astype(str)

        header = pd.merge(
            quarterly_totals,
            quarterly_goals,
            left_on=['ownerRepId', 'Quarter'],
            right_on=['Rep ID', 'Quarter'],
            how='left'
        )

        header = header.drop(columns=['ownerRepId'], errors='ignore')
        header = header[header['Meta'].notna() & (header['Meta'] > 0)]

        header['Cumplimiento'] = header['Amount in company currency'] / header['Meta']

        return header

    @staticmethod
    def _merge_with_eligibility(
        header: pd.DataFrame,
        eligibility: pd.DataFrame
    ) -> pd.DataFrame:
        merged = pd.merge(
            header,
            eligibility,
            left_on=['Rep ID', 'Quarter'],
            right_on=['ownerRepId', 'Quarter'],
            how='left'
        )

        merged = merged.drop(columns=['ownerRepId'], errors='ignore')
        merged['Cumplimiento Todos los Meses'] = merged['Cumplimiento Todos los Meses'].fillna(False)

        return merged

    @staticmethod
    def _calculate_quarterly_bonus(header: pd.DataFrame) -> pd.DataFrame:
        header = header.copy()

        header['Subtotal'] = np.where(
            (header['Cumplimiento'] >= 1.0) & (header['Cumplimiento Todos los Meses']),
            header['Amount in company currency'] * QUARTERLY_BONUS_RATE,
            0
        )

        return header

    @staticmethod
    def _format_output(header: pd.DataFrame) -> pd.DataFrame:
        if header.empty:
            return pd.DataFrame()

        header = header.copy()

        header['Fecha'] = header['Quarter'].dt.strftime('%Y-%m-%d')

        output_columns = [
            'Rep ID',
            'Fecha',
            'Amount in company currency',
            'Meta',
            'Cumplimiento',
            'Cumplimiento Todos los Meses',
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
            'Cumplimiento Todos los Meses': 'boolean',
            'Subtotal': 'money',
        }

        return result
