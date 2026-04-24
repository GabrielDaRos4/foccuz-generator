import logging
from abc import abstractmethod
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.context.commissions.domain.ports import ProcessingStrategy
from src.context.commissions.infrastructure.processing_strategies.custom.scania.shared.column_finder import (
    ColumnFinder,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.shared.output_config import (
    OutputConfig,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.shared.threshold_calculator import (
    ThresholdCalculator,
)

logger = logging.getLogger(__name__)


class BaseScaniaStrategy(ProcessingStrategy):

    DAYS_PER_MONTH = 30

    def __init__(
        self,
        role_filter: list[str] = None,
        target_period: str = None,
        output_config: OutputConfig = None,
        show_zero_commission: bool = False,
        include_inactive: bool = False,
        **kwargs
    ):
        self._role_filter = [r.lower().strip() for r in (role_filter or [])]
        self._target_period = self._parse_target_period(target_period)
        self._output_config = output_config
        self._show_zero_commission = show_zero_commission
        self._include_inactive = include_inactive
        self._column_finder = ColumnFinder()
        self._threshold_calculator = ThresholdCalculator()

    def _parse_target_period(self, target_period: str) -> datetime:
        if target_period:
            try:
                return datetime.strptime(target_period, "%Y-%m-%d")
            except ValueError:
                pass
        return datetime.now()

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            logger.warning("Empty dataframe received, returning empty result")
            return self._create_empty_result()

        secondary_arrays = data.attrs.get('secondary_arrays', {})

        employees_df = data.copy()
        employees_df.columns = employees_df.columns.str.lower().str.strip()
        employees_df.attrs['secondary_arrays'] = secondary_arrays

        employees_df = self._remove_duplicate_columns(employees_df)

        employees_df, active_diagnostics = self._filter_active_employees_in_period(employees_df)

        if employees_df.empty:
            logger.warning("No active employees found in target period")
            empty_result = self._create_empty_result()
            empty_result.attrs['diagnostics'] = active_diagnostics
            return empty_result

        filtered_employees, filter_diagnostics = self._filter_by_role_with_diagnostics(employees_df)

        if filtered_employees.empty:
            logger.warning(f"No employees found for role filter: {self._role_filter}")
            empty_result = self._create_empty_result()
            filter_diagnostics.update(active_diagnostics)
            empty_result.attrs['diagnostics'] = filter_diagnostics
            return empty_result

        logger.info(f"Filtered to {len(filtered_employees)} employees")

        filtered_employees = self._prepare_common_fields(filtered_employees)

        result = self._calculate_plan_commission(filtered_employees)

        result = self._format_output(result)

        logger.info(f"Commission calculation completed with {len(result)} records")

        return result

    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        duplicated_mask = df.columns.duplicated()
        if duplicated_mask.any():
            keep_mask = ~duplicated_mask
            return df.iloc[:, keep_mask.tolist()]
        return df

    def _filter_active_employees_in_period(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'filtered_inactive': 0,
            'filtered_not_started': 0,
        }

        original_attrs = df.attrs.copy()
        result = df.copy()
        result.attrs = original_attrs

        if "employee_status" in result.columns and not self._include_inactive:
            active_mask = result["employee_status"].str.lower() == "activo"
            inactive_count = (~active_mask).sum()
            result = result[active_mask].copy()
            result.attrs = original_attrs
            diagnostics['filtered_inactive'] = int(inactive_count)
            logger.info(f"Filtered {inactive_count} inactive employees, {len(result)} remaining")
        elif self._include_inactive:
            logger.info("Including inactive employees (include_inactive=True)")

        result, not_started_count = self._filter_employees_started_before_period(result)
        result.attrs = original_attrs
        diagnostics['filtered_not_started'] = not_started_count

        diagnostics['remaining_rows'] = len(result)

        return result, diagnostics

    def _filter_employees_started_before_period(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, int]:
        if df.empty:
            return df, 0

        active_since_col = next(
            (c for c in df.columns if "active_since" in c.lower()), None
        )

        if not active_since_col:
            return df, 0

        result = df.copy()
        period_end = self._target_period.replace(day=28) + pd.DateOffset(days=4)
        period_end = period_end.replace(day=1) - pd.DateOffset(days=1)

        active_since_dates = pd.to_datetime(result[active_since_col], errors="coerce")

        started_before_period = active_since_dates.isna() | (active_since_dates <= period_end)
        not_started_count = (~started_before_period).sum()

        if not_started_count > 0:
            logger.info(
                f"Filtered {not_started_count} employees who started after period "
                f"(active_since > {period_end.strftime('%Y-%m-%d')})"
            )

        return result[started_before_period].copy(), int(not_started_count)

    def _filter_by_role(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered, _ = self._filter_by_role_with_diagnostics(df)
        return filtered

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': self._role_filter,
            'filtered_out_by_role': 0,
            'available_roles': [],
        }

        role_col = self._find_role_column(df)
        if not role_col:
            logger.warning("No role column found, returning all employees")
            diagnostics['no_role_column'] = True
            return df.copy(), diagnostics

        role_values = df[role_col].astype(str).str.strip().str.lower()
        diagnostics['available_roles'] = role_values.unique().tolist()

        if not self._role_filter:
            result = df.copy()
            result.attrs = df.attrs.copy()
            return result, diagnostics

        normalized_filters = [f.strip().lower() for f in self._role_filter]
        mask = role_values.isin(normalized_filters)
        filtered_df = df[mask].copy()
        filtered_df.attrs = df.attrs.copy()

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        return filtered_df, diagnostics

    def _find_role_column(self, df: pd.DataFrame) -> str | None:
        role_col = self._column_finder.find_by_pattern(
            df, "cargo", "position", "job_title", "job_name", "role", "puesto"
        )
        if role_col and "cargo2" in role_col.lower():
            for col in df.columns:
                if "cargo" in col.lower() and "cargo2" not in col.lower():
                    return col
            return None
        return role_col

    def _prepare_common_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        original_attrs = df.attrs.copy()
        result = df.copy()
        result.attrs = original_attrs

        result = self._prepare_branch_id(result)
        result.attrs = original_attrs
        result = self._prepare_days_worked(result)
        result.attrs = original_attrs

        return result

    def _prepare_branch_id(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        branch_col = next((c for c in result.columns if "branch" in c and "id" in c), None)
        if branch_col:
            result["branch_id"] = result[branch_col].astype(str).str.strip()
            result = result[
                result["branch_id"].notna() &
                (result["branch_id"].str.len() > 0) &
                (result["branch_id"].str.lower() != "nan")
            ].copy()
        return result

    def _prepare_days_worked(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        days_col = self._column_finder.find_days_column(result)
        if not days_col:
            days_col = self._column_finder.find_by_pattern(
                result, "nodiastrabajados", "worked_days"
            )

        if days_col:
            result["days_worked"] = pd.to_numeric(
                result[days_col], errors="coerce"
            ).fillna(0)
            result.loc[result["days_worked"] < 0, "days_worked"] = 0
        else:
            result["days_worked"] = self.DAYS_PER_MONTH

        result["days_worked"] = result["days_worked"].astype(int)
        return result

    @abstractmethod
    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def _apply_days_proration(self, df: pd.DataFrame, amount_col: str) -> pd.DataFrame:
        result = df.copy()
        result["final_amount"] = (
            result[amount_col] * result["days_worked"] / self.DAYS_PER_MONTH
        ).round(0).astype(int)
        result["commission"] = result["final_amount"]
        return result

    BASE_OUTPUT_COLUMNS = [
        "Fecha", "Rep ID", "ID Transacción"
    ]

    COLUMN_RENAME_MAP = {
        "sales": "Venta",
        "target": "Meta",
        "sales_compliance": "Cumplimiento Venta",
        "compliance": "Cumplimiento Venta",
        "commission_payment": "Pago Cumplimiento Venta",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Días Trabajados", "Monto Final", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    def _format_output(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        period_dt = self._target_period
        result["Fecha"] = period_dt.strftime("%Y-%m-%d")

        result = self._rename_employee_id_column(result)
        result = self._create_transaction_id(result)
        result = self._add_period_columns(result, period_dt)
        result = self._rename_output_columns(result)
        result = self._rename_calculation_columns(result)
        result = self._select_output_columns(result)
        result = self._filter_valid_rows(result)
        result = self._apply_column_types(result)

        return result

    def _apply_column_types(self, df: pd.DataFrame) -> pd.DataFrame:
        column_types = {}
        type_source = (
            self._output_config.column_types if self._output_config
            else self.COLUMN_TYPES
        )
        for col in df.columns:
            if col in type_source:
                column_types[col] = type_source[col]
        df.attrs['column_types'] = column_types
        return df

    def _rename_calculation_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        rename_source = (
            self._output_config.column_rename_map if self._output_config
            else self.COLUMN_RENAME_MAP
        )
        rename_map = {}
        for old_col, new_col in rename_source.items():
            if old_col in result.columns:
                rename_map[old_col] = new_col
        return result.rename(columns=rename_map)

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        output_cols = []

        for col in self.BASE_OUTPUT_COLUMNS:
            if col in df.columns:
                output_cols.append(col)

        plan_cols = (
            self._output_config.output_columns if self._output_config
            else self.PLAN_OUTPUT_COLUMNS
        )
        for col in plan_cols:
            if col in df.columns and col not in output_cols:
                output_cols.append(col)

        return df[output_cols].copy()

    def _rename_employee_id_column(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        id_col = next((c for c in result.columns if "id" in c and "empleado" in c), None)
        if id_col:
            result = result.rename(columns={id_col: "Rep ID"})
        elif "id empleado" in result.columns:
            result = result.rename(columns={"id empleado": "Rep ID"})
        return result

    def _create_transaction_id(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        if "Rep ID" in result.columns:
            result["ID Transacción"] = (
                result["Fecha"].astype(str) + "_" + result["Rep ID"].astype(str)
            )
        else:
            result["ID Transacción"] = result["Fecha"].astype(str) + "_" + result.index.astype(str)
        return result

    def _add_period_columns(self, df: pd.DataFrame, period_dt: datetime) -> pd.DataFrame:
        result = df.copy()
        payment_month_dt = period_dt + relativedelta(months=1)
        result["Mes Pago"] = payment_month_dt.strftime("%b/%Y")
        result["Periodo"] = period_dt.strftime("%b/%Y")
        return result

    def _rename_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        rut_col = next((c for c in result.columns if "rut" in c and "match" not in c), None)
        if rut_col and rut_col != "Rut":
            result = result.rename(columns={rut_col: "Rut"})

        if "branch_id" in result.columns and "Site_ID" not in result.columns:
            result = result.rename(columns={"branch_id": "Site_ID"})

        branch_name_col = next((c for c in result.columns if c == "branch"), None)
        if branch_name_col and branch_name_col != "Sucursal":
            result = result.rename(columns={branch_name_col: "Sucursal"})

        role_col = next((c for c in result.columns if "cargo" in c and "cargo2" not in c), None)
        if role_col and role_col != "Cargo":
            result = result.rename(columns={role_col: "Cargo"})

        if "commission" in result.columns:
            result = result.rename(columns={"commission": "Comisión"})

        return result

    def _filter_valid_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        for col in ["Fecha", "Rep ID", "ID Transacción", "Rut"]:
            if col in result.columns:
                result = result[
                    result[col].notna() &
                    (result[col] != "") &
                    (result[col] != "nan")
                ]

        result = self._filter_rows_with_data(result)
        return result

    def _filter_rows_with_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        if self._show_zero_commission:
            return df

        commission_col = "Comisión" if "Comisión" in df.columns else "commission"
        if commission_col in df.columns:
            commission_values = pd.to_numeric(df[commission_col], errors="coerce").fillna(0)
            has_commission = commission_values != 0
            return df[has_commission].copy()

        return df

    def _create_empty_result(self) -> pd.DataFrame:
        return pd.DataFrame(columns=[
            "Fecha", "Rep ID", "ID Transacción", "Periodo", "Mes Pago",
            "Rut", "Site_ID", "Sucursal", "Cargo", "Comisión"
        ])

    def calculate_compliance_payment(self, compliance: float, thresholds: list[tuple]) -> int:
        if pd.isna(compliance):
            return 0
        pct = compliance * 100
        return self._threshold_calculator.calculate_payment(pct, thresholds)
