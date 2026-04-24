import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .output_formatter import GrupoKOutputFormatter

logger = logging.getLogger(__name__)


class GrupoKSalesAdvisorStrategy(ProcessingStrategy):

    def __init__(
        self,
        target_period: str = None,
        rep_id_filter: str = None,
        role_filter: str = None
    ):
        self._target_period = target_period
        self._rep_id_filter = rep_id_filter
        self._role_filter = role_filter
        self._output_formatter = GrupoKOutputFormatter()

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        employees = data.attrs.get('employees')
        sales = data.attrs.get('sales')
        tiers = data.attrs.get('commission_tiers')

        if employees is None or employees.empty:
            logger.warning("No employees data found")
            return pd.DataFrame()

        if sales is None or sales.empty:
            logger.warning("No sales data found")
            return pd.DataFrame()

        if tiers is None or tiers.empty:
            logger.warning("No commission tiers data found")
            return pd.DataFrame()

        filtered_employees = self._filter_employees_by_role(employees)
        if filtered_employees.empty:
            logger.warning(f"No employees found with role: {self._role_filter}")
            return pd.DataFrame()

        logger.info(f"Found {len(filtered_employees)} employees with role: {self._role_filter}")

        employee_ruts = self._extract_employee_ruts(filtered_employees)

        sales = self._normalize_sales_columns(sales)
        filtered_sales = self._filter_sales_by_employees(sales, employee_ruts)

        if filtered_sales.empty:
            logger.warning("No sales found for filtered employees")
            return pd.DataFrame()

        logger.info(f"Found {len(filtered_sales)} sales records for filtered employees")

        if self._target_period:
            filtered_sales = self._filter_sales_by_period(filtered_sales)
            logger.info(f"After period filter: {len(filtered_sales)} sales records")

        result = self._calculate_commissions(filtered_sales, tiers, filtered_employees)

        if self._rep_id_filter:
            result = self._filter_by_rep_id(result)

        return self._output_formatter.format(result)

    def _filter_employees_by_role(self, employees: pd.DataFrame) -> pd.DataFrame:
        if self._role_filter is None:
            return employees

        filtered = []
        for _, emp in employees.iterrows():
            current_job = emp.get('current_job')
            if current_job and isinstance(current_job, dict):
                role = current_job.get('role', {})
                if role and isinstance(role, dict):
                    role_name = role.get('name', '')
                    if self._role_filter.lower() in role_name.lower():
                        filtered.append(emp)

        return pd.DataFrame(filtered) if filtered else pd.DataFrame()

    def _extract_employee_ruts(self, employees: pd.DataFrame) -> dict[str, dict]:
        ruts = {}
        for _, emp in employees.iterrows():
            rut = emp.get('rut', '')
            if rut:
                sanitized_rut = self._sanitize_rut(str(rut))
                ruts[sanitized_rut] = {
                    'id': emp.get('id'),
                    'full_name': emp.get('full_name', ''),
                    'rut': rut,
                }
        return ruts

    @staticmethod
    def _sanitize_rut(rut: str) -> str:
        return rut.replace('.', '').replace(' ', '').strip().upper()

    @staticmethod
    def _normalize_sales_columns(sales: pd.DataFrame) -> pd.DataFrame:
        sales = sales.copy()
        sales.columns = sales.columns.str.lower().str.strip()

        if 'rut_vendedor' in sales.columns:
            sales['rut_vendedor_sanitized'] = (
                sales['rut_vendedor']
                .astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(' ', '', regex=False)
                .str.strip()
                .str.upper()
            )

        return sales

    @staticmethod
    def _filter_sales_by_employees(
        sales: pd.DataFrame,
        employee_ruts: dict[str, dict]
    ) -> pd.DataFrame:
        if 'rut_vendedor_sanitized' not in sales.columns:
            return pd.DataFrame()

        mask = sales['rut_vendedor_sanitized'].isin(employee_ruts.keys())
        return sales[mask].copy()

    def _filter_sales_by_period(self, sales: pd.DataFrame) -> pd.DataFrame:
        if 'fecha' not in sales.columns:
            return sales

        sales['parsed_date'] = pd.to_datetime(sales['fecha'], errors='coerce')

        target = self._parse_target_period()
        if target is None:
            return sales

        mask = (
            (sales['parsed_date'].dt.year == target.year) &
            (sales['parsed_date'].dt.month == target.month)
        )

        return sales[mask].copy()

    def _parse_target_period(self) -> datetime | None:
        if not self._target_period:
            return None

        try:
            if isinstance(self._target_period, datetime):
                return self._target_period
            return datetime.strptime(str(self._target_period), '%Y-%m-%d')
        except ValueError:
            return None

    def _calculate_commissions(
        self,
        sales: pd.DataFrame,
        tiers: pd.DataFrame,
        employees: pd.DataFrame
    ) -> pd.DataFrame:
        if 'monto_neto' in sales.columns:
            sales['net_amount_num'] = pd.to_numeric(
                sales['monto_neto'], errors='coerce'
            ).fillna(0)
        else:
            sales['net_amount_num'] = 0

        sales['commission_per_sale'] = sales['net_amount_num'].apply(
            lambda amount: amount * (self._get_commission_rate(amount, tiers) / 100)
        )

        first_day_str = self._get_period_first_day(sales)

        aggregated = sales.groupby('rut_vendedor_sanitized').agg(
            Vendedor=('vendedor', 'first'),
            Sucursal=('sucursal', 'first'),
            Cantidad_Ventas=('net_amount_num', 'count'),
            Monto_Neto=('net_amount_num', 'sum'),
            Comision=('commission_per_sale', 'sum')
        ).reset_index()

        aggregated.rename(columns={'rut_vendedor_sanitized': 'Rep ID'}, inplace=True)
        aggregated['Fecha'] = first_day_str
        aggregated['ID Transaccion'] = aggregated.apply(
            lambda row: f"GK_{row['Rep ID']}_{first_day_str.replace('-', '')}",
            axis=1
        )
        aggregated.rename(columns={
            'Cantidad_Ventas': 'Cantidad Ventas',
            'Monto_Neto': 'Monto Neto'
        }, inplace=True)

        return aggregated

    @staticmethod
    def _get_period_first_day(sales: pd.DataFrame) -> str:
        if 'parsed_date' in sales.columns and not sales['parsed_date'].isna().all():
            first_date = sales['parsed_date'].dropna().iloc[0]
            return first_date.replace(day=1).strftime('%Y-%m-%d')
        return ''

    @staticmethod
    def _get_commission_rate(amount: float, tiers: pd.DataFrame) -> float:
        for _, tier in tiers.iterrows():
            from_value = float(tier.get('desde', 0))
            to_value = tier.get('hasta')
            rate = float(tier.get('comision_bruta', 0))

            if to_value is None or pd.isna(to_value):
                if amount >= from_value:
                    return rate
            else:
                to_value = float(to_value)
                if from_value <= amount <= to_value:
                    return rate

        return 0.0

    def _filter_by_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_id_str = self._sanitize_rut(str(self._rep_id_filter))
        filtered = df[df['Rep ID'].astype(str).str.upper() == rep_id_str].copy()
        logger.info(f"Filtered by Rep ID {rep_id_str}: {len(filtered)} records")
        return filtered

    def get_column_types(self) -> dict[str, str]:
        return self._output_formatter.COLUMN_TYPES
