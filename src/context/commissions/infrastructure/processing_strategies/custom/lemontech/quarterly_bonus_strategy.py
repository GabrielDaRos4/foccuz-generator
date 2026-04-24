import logging
from datetime import datetime

import numpy as np
import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

logger = logging.getLogger(__name__)

DATA_START_DATE = '2025-01-01'
TIMEZONE_OFFSET_HOURS = 4
MONTHS_PER_QUARTER = 3

MIN_MONTHLY_COMPLIANCE = 0.6
QUARTERLY_BONUS_RATE = 0.2


class LemontechQuarterlyBonusStrategy(ProcessingStrategy):

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

        goals_df = self._prepare_goals(goals_df)

        reps = goals_df['Rep ID'].unique()
        deals = deals[deals['ownerRepId'].isin(reps)]

        deals['Comisión'] = 0

        if self._rep_id_filter:
            deals = self._filter_by_rep_id(deals)

        result = self._format_output(deals)

        return result

    @staticmethod
    def _prepare_deals(data: pd.DataFrame) -> pd.DataFrame:
        deals = data.copy()

        deals['closeDate'] = pd.to_datetime(deals['closeDate']) - pd.Timedelta(hours=TIMEZONE_OFFSET_HOURS)
        deals = deals[deals['closeDate'] >= DATA_START_DATE]

        deals['Amount in company currency'] = pd.to_numeric(
            deals['Amount in company currency'], errors='coerce'
        ).fillna(0)

        deals = deals[deals['ownerRepId'].notna()]
        deals['ownerRepId'] = deals['ownerRepId'].astype(int).astype(str)

        deals['Fecha'] = deals['closeDate'].dt.to_period('M').dt.to_timestamp()
        deals['Quarter'] = deals['closeDate'].dt.to_period('Q').dt.end_time

        logger.info(f"Prepared {len(deals)} valid deals for quarterly analysis")
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

        quarterly_check['all_months_met'] = quarterly_check['met_minimum'] == MONTHS_PER_QUARTER

        return quarterly_check[['ownerRepId', 'Quarter', 'all_months_met']]

    @staticmethod
    def _calculate_quarterly_summary(
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

        merged = pd.merge(
            quarterly_totals,
            quarterly_goals,
            left_on=['ownerRepId', 'Quarter'],
            right_on=['Rep ID', 'Quarter'],
            how='left'
        )

        merged = merged.drop(columns=['Rep ID'], errors='ignore')
        merged = merged[merged['Meta'].notna() & (merged['Meta'] > 0)]

        merged['Cumplimiento'] = merged['Amount in company currency'] / merged['Meta']

        return merged

    @staticmethod
    def _merge_with_eligibility(
        summary: pd.DataFrame,
        eligibility: pd.DataFrame
    ) -> pd.DataFrame:
        merged = pd.merge(
            summary,
            eligibility,
            on=['ownerRepId', 'Quarter'],
            how='left'
        )

        merged['all_months_met'] = merged['all_months_met'].fillna(False)

        return merged

    @staticmethod
    def _calculate_quarterly_bonus(summary: pd.DataFrame) -> pd.DataFrame:
        summary = summary.copy()

        summary['Subtotal'] = np.where(
            (summary['Cumplimiento'] >= 1.0) & (summary['all_months_met']),
            summary['Amount in company currency'] * QUARTERLY_BONUS_RATE,
            0
        )

        return summary

    def _filter_by_period(self, summary: pd.DataFrame) -> pd.DataFrame:
        target = self._parse_target_period()
        if target is None:
            return summary

        target_quarter = pd.Timestamp(target).to_period('Q').end_time

        filtered = summary[summary['Quarter'] == target_quarter].copy()
        logger.info(f"Filtered to {len(filtered)} records for quarter ending {target_quarter}")
        return filtered

    def _parse_target_period(self) -> datetime | None:
        if not self._target_period:
            return None

        try:
            if isinstance(self._target_period, datetime):
                return self._target_period
            return datetime.strptime(str(self._target_period), '%Y-%m-%d')
        except ValueError:
            return None

    def _filter_by_rep_id(self, summary: pd.DataFrame) -> pd.DataFrame:
        rep_id = str(self._rep_id_filter).strip()
        filtered = summary[summary['ownerRepId'] == rep_id].copy()
        logger.info(f"Filtered by Rep ID {rep_id}: {len(filtered)} records")
        return filtered

    @staticmethod
    def _format_output(deals: pd.DataFrame) -> pd.DataFrame:
        if deals.empty:
            return pd.DataFrame()

        deals = deals.copy()

        deals['Fecha'] = deals['closeDate'].dt.to_period('Q').dt.end_time
        deals['Fecha'] = deals['Fecha'].apply(lambda x: x.strftime('%Y-%m-%d'))
        deals['closeDate'] = deals['closeDate'].dt.strftime('%Y-%m-%d')
        deals['pipelineLabel'] = deals['pipelineLabel'].fillna('No').replace('', 'No')

        output = deals.rename(columns={
            'id': 'ID Transacción',
            'ownerRepId': 'Rep ID',
        })

        output_columns = [
            'ID Transacción',
            'Rep ID',
            'Tipo de Venta',
            'closeDate',
            'name',
            'Amount in company currency',
            'pipelineLabel',
            'Tipo de Cobro',
            'Opp Type',
            'Fecha',
            'Comisión',
        ]

        existing_cols = [c for c in output_columns if c in output.columns]
        result = output[existing_cols].copy()

        result.attrs['column_types'] = {
            'ID Transacción': 'text',
            'Rep ID': 'text',
            'Tipo de Venta': 'text',
            'closeDate': 'text',
            'name': 'text',
            'Amount in company currency': 'money',
            'pipelineLabel': 'text',
            'Tipo de Cobro': 'text',
            'Opp Type': 'text',
            'Fecha': 'text',
            'Comisión': 'money',
        }

        return result
