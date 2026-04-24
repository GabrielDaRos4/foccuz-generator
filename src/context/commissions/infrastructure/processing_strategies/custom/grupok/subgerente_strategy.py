import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .output_formatter import GrupoKOutputFormatter

logger = logging.getLogger(__name__)

SUBGERENTE_ROLE_CODE = 'subgerente_de_producto'


class GrupoKSubgerenteStrategy(ProcessingStrategy):

    def __init__(
        self,
        target_period: str = None,
        rep_id_filter: str = None,
        pm_role_code: str = None,
        sales_columns: list[str] = None,
        subgerente_ruts: list[str] = None
    ):
        self._target_period = target_period
        self._rep_id_filter = rep_id_filter
        self._pm_role_code = pm_role_code
        self._sales_columns = sales_columns or []
        self._subgerente_ruts = self._sanitize_rut_list(subgerente_ruts or [])
        self._output_formatter = GrupoKOutputFormatter()

    @staticmethod
    def _sanitize_rut_list(ruts: list[str]) -> list[str]:
        return [r.replace('.', '').replace(' ', '').strip().upper() for r in ruts]

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        employees = data.attrs.get('employees')
        sales = data.attrs.get('sales')
        tiers = data.attrs.get('commission_tiers')

        if not self._validate_data(employees, sales, tiers):
            return pd.DataFrame()

        pm_employee = self._find_product_manager(employees)
        if pm_employee is None:
            logger.warning(f"No PM found with role_code: {self._pm_role_code}")
            return pd.DataFrame()

        subgerentes = self._find_subgerentes(employees, pm_employee)
        if not subgerentes:
            logger.warning(f"No subgerentes found for PM: {pm_employee['full_name']}")
            return pd.DataFrame()

        sales = self._prepare_sales_data(sales)
        total_sales = self._calculate_total_sales(sales)

        results = []
        for subgerente in subgerentes:
            commission = self._get_fixed_commission(total_sales, tiers)
            result = self._build_result(
                subgerente,
                pm_employee,
                total_sales,
                commission,
                len(sales)
            )
            results.append(result)

        if not results:
            return pd.DataFrame()

        combined = pd.concat(results, ignore_index=True)

        if self._rep_id_filter:
            combined = self._filter_by_rep_id(combined)

        return self._output_formatter.format_subgerente(combined)

    @staticmethod
    def _validate_data(
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

    def _find_product_manager(self, employees: pd.DataFrame) -> dict | None:
        for _, emp in employees.iterrows():
            if self._employee_matches_role_code(emp, self._pm_role_code):
                return self._build_employee_dict(emp)
        return None

    def _find_subgerentes(
        self,
        employees: pd.DataFrame,
        pm_employee: dict
    ) -> list[dict]:
        subgerentes = []
        pm_rut = pm_employee['sanitized_rut']

        for _, emp in employees.iterrows():
            if not self._employee_matches_role_code(emp, SUBGERENTE_ROLE_CODE):
                continue

            boss = self._get_boss_from_employee(emp)
            if not boss:
                continue

            boss_rut = boss.get('rut', '')
            sanitized_boss_rut = boss_rut.replace('.', '').replace(' ', '').strip().upper()

            if sanitized_boss_rut == pm_rut:
                emp_dict = self._build_employee_dict(emp)
                if self._matches_subgerente_filter(emp_dict):
                    subgerentes.append(emp_dict)

        logger.info(f"Found {len(subgerentes)} subgerentes for PM {pm_employee['full_name']}")
        return subgerentes

    def _matches_subgerente_filter(self, employee: dict) -> bool:
        if not self._subgerente_ruts:
            return True
        return employee['sanitized_rut'] in self._subgerente_ruts

    @staticmethod
    def _get_boss_from_employee(employee: pd.Series) -> dict | None:
        current_job = employee.get('current_job')
        if current_job and isinstance(current_job, dict):
            boss = current_job.get('boss')
            if boss and isinstance(boss, dict):
                return boss
        return None

    @staticmethod
    def _employee_matches_role_code(employee: pd.Series, role_code: str) -> bool:
        current_job = employee.get('current_job')
        if not current_job or not isinstance(current_job, dict):
            return False
        role = current_job.get('role', {})
        if not role or not isinstance(role, dict):
            return False
        emp_role_code = role.get('code', '')
        return emp_role_code.lower() == role_code.lower()

    @staticmethod
    def _build_employee_dict(employee: pd.Series) -> dict:
        rut = employee.get('rut', '')
        return {
            'id': employee.get('id'),
            'full_name': employee.get('full_name', ''),
            'rut': rut,
            'sanitized_rut': rut.replace('.', '').replace(' ', '').strip().upper(),
        }

    def _prepare_sales_data(self, sales: pd.DataFrame) -> pd.DataFrame:
        sales = sales.copy()
        sales.columns = sales.columns.str.lower().str.strip()

        if self._target_period:
            sales = self._filter_sales_by_period(sales)
            logger.info(f"After period filter: {len(sales)} sales records")

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

    def _calculate_total_sales(self, sales: pd.DataFrame) -> float:
        total = 0.0
        for col in self._sales_columns:
            col_lower = col.lower()
            if col_lower in sales.columns:
                col_total = pd.to_numeric(sales[col_lower], errors='coerce').fillna(0).sum()
                total += col_total
                logger.info(f"Column {col}: {col_total:,.0f}")
        logger.info(f"Total sales for subgerente: {total:,.0f}")
        return total

    @staticmethod
    def _get_fixed_commission(total_sales: float, tiers: pd.DataFrame) -> float:
        for _, tier in tiers.iterrows():
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
        return 0.0

    def _build_result(
        self,
        subgerente: dict,
        pm_employee: dict,
        total_sales: float,
        commission: float,
        sales_count: int
    ) -> pd.DataFrame:
        period_date = self._get_period_date()
        record = {
            'Fecha': period_date,
            'Rep ID': subgerente['sanitized_rut'],
            'ID Transaccion': f"GK_SG_{subgerente['sanitized_rut']}_{period_date.replace('-', '')}",
            'Subgerente': subgerente['full_name'],
            'RUT': subgerente['rut'],
            'Product Manager': pm_employee['full_name'],
            'Linea Negocio': ', '.join(self._sales_columns),
            'Cantidad Ventas': sales_count,
            'Total Ventas': total_sales,
            'Comision': commission,
        }
        return pd.DataFrame([record])

    def _get_period_date(self) -> str:
        target = self._parse_target_period()
        if target:
            return target.replace(day=1).strftime('%Y-%m-%d')
        return datetime.now().replace(day=1).strftime('%Y-%m-%d')

    def _filter_by_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_id_str = str(self._rep_id_filter).replace('.', '').replace(' ', '').strip().upper()
        filtered = df[df['Rep ID'].astype(str).str.upper() == rep_id_str].copy()
        logger.info(f"Filtered by Rep ID {rep_id_str}: {len(filtered)} records")
        return filtered

    def get_column_types(self) -> dict[str, str]:
        return self._output_formatter.SUBGERENTE_COLUMN_TYPES
