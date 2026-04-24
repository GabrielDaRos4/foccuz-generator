import logging
import unicodedata
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .output_formatter import GrupoKOutputFormatter

logger = logging.getLogger(__name__)


class GrupoKStoreManagerStrategy(ProcessingStrategy):

    STORE_MANAGER_ROLE = 'Jefe de Tienda'
    STORE_LOCATION_ATTR = 'Lugar de trabajo (Sucursal)'

    def __init__(
        self,
        target_period: str = None,
        rep_id_filter: str = None,
        role_filter: str = None
    ):
        self._target_period = target_period
        self._rep_id_filter = rep_id_filter
        self._role_filter = role_filter or self.STORE_MANAGER_ROLE
        self._output_formatter = GrupoKOutputFormatter()

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        employees, sales, tiers = self._extract_data_sources(data)
        if not self._validate_data_sources(employees, sales, tiers):
            return pd.DataFrame()

        managers_with_stores = self._get_managers_with_stores(employees)
        if not managers_with_stores:
            return pd.DataFrame()

        sales = self._prepare_sales_data(sales)
        result = self._calculate_store_commissions(sales, tiers, managers_with_stores)

        if self._rep_id_filter:
            result = self._filter_by_rep_id(result)

        return self._output_formatter.format_store_manager(result)

    @staticmethod
    def _extract_data_sources(data: pd.DataFrame) -> tuple:
        employees = data.attrs.get('employees')
        sales = data.attrs.get('sales')
        tiers = data.attrs.get('commission_tiers')
        return employees, sales, tiers

    @staticmethod
    def _validate_data_sources(
        employees: pd.DataFrame,
        sales: pd.DataFrame,
        tiers: pd.DataFrame
    ) -> bool:
        if employees is None or employees.empty:
            logger.warning("No employees data found")
            return False

        if sales is None or sales.empty:
            logger.warning("No sales data found")
            return False

        if tiers is None or tiers.empty:
            logger.warning("No commission tiers data found")
            return False

        return True

    def _get_managers_with_stores(self, employees: pd.DataFrame) -> list[dict]:
        store_managers = self._filter_employees_by_role(employees)
        if store_managers.empty:
            logger.warning(f"No employees found with role: {self._role_filter}")
            return []

        logger.info(f"Found {len(store_managers)} employees with role: {self._role_filter}")

        managers_with_stores = self._extract_manager_stores(store_managers)
        if not managers_with_stores:
            logger.warning("No store managers with assigned stores found")
            return []

        return managers_with_stores

    def _prepare_sales_data(self, sales: pd.DataFrame) -> pd.DataFrame:
        sales = self._normalize_sales_columns(sales)

        if self._target_period:
            sales = self._filter_sales_by_period(sales)
            logger.info(f"After period filter: {len(sales)} sales records")

        return sales

    def _filter_employees_by_role(self, employees: pd.DataFrame) -> pd.DataFrame:
        if self._role_filter is None:
            return employees

        filtered = []
        for _, emp in employees.iterrows():
            if self._employee_matches_role(emp):
                filtered.append(emp)

        return pd.DataFrame(filtered) if filtered else pd.DataFrame()

    def _employee_matches_role(self, employee: pd.Series) -> bool:
        current_job = employee.get('current_job')
        if not current_job or not isinstance(current_job, dict):
            return False

        role = current_job.get('role', {})
        if not role or not isinstance(role, dict):
            return False

        role_name = role.get('name', '')
        return self._role_filter.lower() in role_name.lower()

    def _extract_manager_stores(self, employees: pd.DataFrame) -> list[dict]:
        managers = []
        for _, emp in employees.iterrows():
            manager_data = self._build_manager_data(emp)
            if manager_data:
                managers.append(manager_data)
        return managers

    def _build_manager_data(self, employee: pd.Series) -> dict | None:
        rut = employee.get('rut', '')
        if not rut:
            return None

        store_location = self._get_store_location(employee)
        if not store_location:
            logger.warning(f"No store location for manager {employee.get('full_name')}")
            return None

        return {
            'id': employee.get('id'),
            'full_name': employee.get('full_name', ''),
            'rut': rut,
            'sanitized_rut': self._sanitize_rut(str(rut)),
            'store_location': store_location,
            'normalized_store': self._normalize_store_name(store_location),
        }

    def _get_store_location(self, employee: pd.Series) -> str | None:
        current_job = employee.get('current_job')
        if not current_job or not isinstance(current_job, dict):
            return None

        custom_attrs = current_job.get('custom_attributes', {})
        if not custom_attrs:
            return None

        return custom_attrs.get(self.STORE_LOCATION_ATTR, '')

    @staticmethod
    def _sanitize_rut(rut: str) -> str:
        return rut.replace('.', '').replace(' ', '').strip().upper()

    @staticmethod
    def _normalize_store_name(name: str) -> str:
        if not name:
            return ''
        normalized = unicodedata.normalize('NFD', name)
        ascii_name = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
        return ascii_name.lower().strip()

    def _normalize_sales_columns(self, sales: pd.DataFrame) -> pd.DataFrame:
        sales = sales.copy()
        sales.columns = sales.columns.str.lower().str.strip()

        if 'sucursal' in sales.columns:
            sales['sucursal_normalized'] = (
                sales['sucursal']
                .astype(str)
                .apply(self._normalize_store_name)
            )

        return sales

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

    def _calculate_store_commissions(
        self,
        sales: pd.DataFrame,
        tiers: pd.DataFrame,
        managers: list[dict]
    ) -> pd.DataFrame:
        sales = self._add_numeric_sales_column(sales)
        period_date = self._get_period_date()

        records = []
        for manager in managers:
            record = self._build_commission_record(sales, tiers, manager, period_date)
            records.append(record)

        return pd.DataFrame(records)

    @staticmethod
    def _add_numeric_sales_column(sales: pd.DataFrame) -> pd.DataFrame:
        if 'monto_neto' in sales.columns:
            sales['net_amount_num'] = pd.to_numeric(
                sales['monto_neto'], errors='coerce'
            ).fillna(0)
        else:
            sales['net_amount_num'] = 0
        return sales

    def _get_period_date(self) -> str:
        target = self._parse_target_period()
        if target:
            return target.replace(day=1).strftime('%Y-%m-%d')
        return datetime.now().replace(day=1).strftime('%Y-%m-%d')

    def _build_commission_record(
        self,
        sales: pd.DataFrame,
        tiers: pd.DataFrame,
        manager: dict,
        period_date: str
    ) -> dict:
        store_sales = self._get_store_sales(sales, manager['normalized_store'])

        total_sales = store_sales['net_amount_num'].sum() if not store_sales.empty else 0
        sales_count = len(store_sales)
        commission = self._get_fixed_commission(total_sales, tiers)

        return {
            'Fecha': period_date,
            'Rep ID': manager['sanitized_rut'],
            'ID Transaccion': self._create_transaction_id(
                manager['sanitized_rut'],
                manager['normalized_store'],
                period_date
            ),
            'Jefe de Tienda': manager['full_name'],
            'RUT': manager['rut'],
            'Sucursal': manager['store_location'],
            'Cantidad Ventas': sales_count,
            'Total Ventas': total_sales,
            'Comision': commission,
        }

    @staticmethod
    def _get_store_sales(sales: pd.DataFrame, normalized_store: str) -> pd.DataFrame:
        if 'sucursal_normalized' not in sales.columns:
            return pd.DataFrame()

        mask = sales['sucursal_normalized'].str.contains(
            normalized_store,
            case=False,
            na=False
        ) | sales['sucursal_normalized'].apply(
            lambda x: normalized_store in x if x else False
        )

        return sales[mask].copy()

    def _get_fixed_commission(self, total_sales: float, tiers: pd.DataFrame) -> float:
        for _, tier in tiers.iterrows():
            commission = self._check_tier_match(total_sales, tier)
            if commission is not None:
                return commission
        return 0.0

    @staticmethod
    def _check_tier_match(total_sales: float, tier: pd.Series) -> float | None:
        from_value = float(tier.get('desde', 0))
        to_value = tier.get('hasta')
        commission = float(tier.get('comision_bruta', 0))

        if to_value is None or pd.isna(to_value):
            if total_sales >= from_value:
                return commission
        else:
            to_value = float(to_value)
            if from_value <= total_sales <= to_value:
                return commission

        return None

    @staticmethod
    def _create_transaction_id(rut: str, store: str, date_str: str) -> str:
        rut_clean = rut.replace('-', '')[:10] if rut else 'NORUT'
        store_clean = store.replace(' ', '_')[:15] if store else 'NOSTORE'
        date_clean = date_str.replace('-', '') if date_str else 'NODATE'
        return f"GK_JT_{rut_clean}_{store_clean}_{date_clean}"

    def _filter_by_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_id_str = self._sanitize_rut(str(self._rep_id_filter))
        filtered = df[df['Rep ID'].astype(str).str.upper() == rep_id_str].copy()
        logger.info(f"Filtered by Rep ID {rep_id_str}: {len(filtered)} records")
        return filtered

    def get_column_types(self) -> dict[str, str]:
        return self._output_formatter.STORE_MANAGER_COLUMN_TYPES
