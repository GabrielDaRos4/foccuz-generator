import logging
from datetime import datetime

import numpy as np
import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

logger = logging.getLogger(__name__)

DATA_START_DATE = '2025-01-01'
TIMEZONE_OFFSET_HOURS = 4

ALB_EXCEPTIONS = [
    'ALB ABOGADOS_MEX_CTFirms_100C_INB',
    'ALB ABOGADOS_MEX_TBX_3TK_INB_PlanProfessional'
]

SPLIT_DEAL_ID = '36653880911'
SPLIT_DEAL_CONFIG = [
    {'id_suffix': '-1', 'owner_rep_id': '41515796', 'commission': 1092},
    {'id_suffix': '-2', 'owner_rep_id': '187828087', 'commission': 546},
]
SPLIT_DEAL_ORIGINAL_AMOUNT = 546

REDUCED_COMMISSION_DEALS = ['38727659431', '46041868366', '46143203875']
REDUCED_COMMISSION_RATE = 0.2

SKIP_ANNUAL_BONUS_DEALS = [
    'Prieto Abogados_CH_TBX_77TK_INB_CROSS',
    'Prieto Abogados_CH_TBX_77TK_INB_CROS',
    'Travieso Evans Arria Rengel & Paz_VZ_TTB_32T',
    'Travieso Evans Arria Rengel & Paz_VZ_TTB_32TK_BDROUT_AlianzaBetsa',
]

FIXED_COMMISSION_DEALS = {
    'Travieso Evans Arria Rengel & Paz_VZ_TTB_32T': 960,
    'Travieso Evans Arria Rengel & Paz_VZ_TTB_32TK_BDROUT_AlianzaBetsa': 960,
}

COMPLIANCE_TIERS = [
    (1.2, 1.65),
    (1.1, 1.45),
    (1.0, 1.35),
    (0.8, 1.0),
    (0.65, 0.7),
    (0.6, 0.7),
    (0.5, 0.5),
    (0.4, 0.5),
    (0.2, 0.25),
]

ANNUAL_BONUS_RATE = 0.15


class LemontechMonthlyCommissionStrategy(ProcessingStrategy):

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

        deals = self._apply_alb_exceptions(deals)
        deals = self._apply_split_deal(deals)

        goals_df = self._prepare_goals(goals_df)

        monthly_summary = self._calculate_monthly_summary(deals)
        monthly_summary = self._merge_with_goals(monthly_summary, goals_df)

        if monthly_summary.empty:
            logger.warning("No data after merging with goals")
            return pd.DataFrame()

        monthly_summary = self._calculate_compliance(monthly_summary)
        monthly_summary = self._calculate_commission_rate(monthly_summary)

        deals = self._merge_deals_with_rates(deals, monthly_summary)
        deals = self._calculate_deal_commissions(deals)

        if self._target_period:
            deals = self._filter_by_period(deals)

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

        logger.info(f"Prepared {len(deals)} valid deals")
        return deals

    @staticmethod
    def _apply_alb_exceptions(deals: pd.DataFrame) -> pd.DataFrame:
        deals = deals.copy()
        deals['Pago ALB (Omitido en Resultados)'] = np.where(
            deals['name'].isin(ALB_EXCEPTIONS),
            deals['Amount in company currency'],
            0
        )
        deals['Amount in company currency'] = np.where(
            deals['Pago ALB (Omitido en Resultados)'] > 0,
            0,
            deals['Amount in company currency']
        )
        return deals

    @staticmethod
    def _apply_split_deal(deals: pd.DataFrame) -> pd.DataFrame:
        deals = deals.copy()

        deals['id'] = deals['id'].astype(str)
        deals['Comisión'] = None

        deals.loc[deals['id'] == SPLIT_DEAL_ID, 'Amount in company currency'] = SPLIT_DEAL_ORIGINAL_AMOUNT

        split_rows = []
        original_deal = deals[deals['id'] == SPLIT_DEAL_ID]

        if not original_deal.empty:
            for config in SPLIT_DEAL_CONFIG:
                new_row = original_deal.copy()
                new_row['id'] = SPLIT_DEAL_ID + config['id_suffix']
                new_row['ownerRepId'] = config['owner_rep_id']
                new_row['Comisión'] = config['commission']
                split_rows.append(new_row)

            if split_rows:
                deals = pd.concat([deals] + split_rows, ignore_index=True)

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
    def _calculate_monthly_summary(deals: pd.DataFrame) -> pd.DataFrame:
        summary = deals.groupby(['ownerRepId', 'Fecha'], as_index=False).agg({
            'Amount in company currency': 'sum'
        })
        return summary

    @staticmethod
    def _merge_with_goals(
        summary: pd.DataFrame,
        goals: pd.DataFrame
    ) -> pd.DataFrame:
        summary['ownerRepId'] = summary['ownerRepId'].astype(str)

        if 'Rep ID' not in goals.columns:
            logger.warning("Goals missing 'Rep ID' column")
            return pd.DataFrame()

        merged = pd.merge(
            summary,
            goals[['Rep ID', 'Fecha', 'Meta']],
            left_on=['ownerRepId', 'Fecha'],
            right_on=['Rep ID', 'Fecha'],
            how='left'
        )

        merged = merged.drop(columns=['Rep ID'], errors='ignore')
        merged = merged[merged['Meta'].notna() & (merged['Meta'] > 0)]

        return merged

    @staticmethod
    def _calculate_compliance(summary: pd.DataFrame) -> pd.DataFrame:
        summary = summary.copy()
        summary['Cumplimiento'] = summary['Amount in company currency'] / summary['Meta']
        return summary

    @staticmethod
    def _calculate_commission_rate(summary: pd.DataFrame) -> pd.DataFrame:
        summary = summary.copy()

        def get_rate(compliance):
            for threshold, rate in COMPLIANCE_TIERS:
                if compliance >= threshold:
                    return rate
            return 0

        summary['% Comision'] = summary['Cumplimiento'].apply(get_rate)
        return summary

    @staticmethod
    def _merge_deals_with_rates(
        deals: pd.DataFrame,
        summary: pd.DataFrame
    ) -> pd.DataFrame:
        deals = deals.copy()
        deals['ownerRepId'] = deals['ownerRepId'].astype(str)

        merged = pd.merge(
            deals,
            summary[['ownerRepId', 'Fecha', '% Comision', 'Cumplimiento', 'Meta']],
            on=['ownerRepId', 'Fecha'],
            how='left'
        )

        merged = merged[merged['% Comision'].notna()]
        return merged

    @staticmethod
    def _calculate_deal_commissions(deals: pd.DataFrame) -> pd.DataFrame:
        deals = deals.copy()

        deals['id'] = deals['id'].astype(str)

        is_split = deals['id'].isin([SPLIT_DEAL_ID + c['id_suffix'] for c in SPLIT_DEAL_CONFIG])
        is_reduced = deals['id'].isin(REDUCED_COMMISSION_DEALS)
        is_fixed_commission = deals['name'].isin(FIXED_COMMISSION_DEALS.keys())

        fixed_commission_values = deals['name'].map(FIXED_COMMISSION_DEALS).fillna(0)

        deals['Monto comisión'] = np.where(
            is_fixed_commission,
            fixed_commission_values,
            np.where(
                is_split & deals['Comisión'].notna(),
                deals['Comisión'],
                np.where(
                    is_reduced,
                    (deals['Amount in company currency'] * REDUCED_COMMISSION_RATE) * deals['% Comision'],
                    deals['Amount in company currency'] * deals['% Comision']
                )
            )
        )

        is_skip_annual = deals['name'].isin(SKIP_ANNUAL_BONUS_DEALS)

        deals['Pago por Anualidad'] = np.where(
            (deals['Tipo de Cobro'] == 'Anual') & (deals['Opp Type'] != 'Add-on') & (~is_skip_annual),
            deals['Monto comisión'] * ANNUAL_BONUS_RATE,
            0
        )

        deals['Comisión'] = (
            deals['Monto comisión'] +
            deals['Pago ALB (Omitido en Resultados)'] +
            deals['Pago por Anualidad']
        )

        return deals

    def _filter_by_period(self, deals: pd.DataFrame) -> pd.DataFrame:
        target = self._parse_target_period()
        if target is None:
            return deals

        mask = (
            (deals['Fecha'].dt.year == target.year) &
            (deals['Fecha'].dt.month == target.month)
        )

        filtered = deals[mask].copy()
        logger.info(f"Filtered to {len(filtered)} deals for period {target}")
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

    def _filter_by_rep_id(self, deals: pd.DataFrame) -> pd.DataFrame:
        rep_id = str(self._rep_id_filter).strip()
        filtered = deals[deals['ownerRepId'] == rep_id].copy()
        logger.info(f"Filtered by Rep ID {rep_id}: {len(filtered)} deals")
        return filtered

    @staticmethod
    def _format_output(deals: pd.DataFrame) -> pd.DataFrame:
        if deals.empty:
            return pd.DataFrame()

        deals = deals.copy()

        deals['closeDate'] = deals['closeDate'].dt.strftime('%Y-%m-%d')
        deals['Fecha'] = deals['Fecha'].dt.strftime('%Y-%m-%d')
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
            'Pago ALB (Omitido en Resultados)',
            'Fecha',
            '% Comision',
            'Monto comisión',
            'Pago por Anualidad',
            'Comisión',
        ]

        existing_cols = [c for c in output_columns if c in output.columns]
        result = output[existing_cols].copy()

        result = result[result['Comisión'].notna()]

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
            'Pago ALB (Omitido en Resultados)': 'money',
            'Fecha': 'text',
            '% Comision': 'percent',
            'Monto comisión': 'money',
            'Pago por Anualidad': 'money',
            'Comisión': 'money',
        }

        return result
