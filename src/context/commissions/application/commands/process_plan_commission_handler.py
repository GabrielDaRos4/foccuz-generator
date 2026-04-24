import logging
import time
from datetime import datetime

import pandas as pd

from src.context.commissions.application.commands.process_plan_commission_command import (
    ProcessPlanCommissionCommand,
)
from src.context.commissions.application.dto import PlanExecutionResult
from src.context.commissions.domain.aggregates import Plan, Tenant
from src.context.commissions.domain.ports import Exporter, StrategyFactory
from src.context.commissions.domain.repositories import MultiSourceDataRepository
from src.context.commissions.domain.services import CommissionCalculatorService
from src.context.shared.domain.cqrs import CommandHandler

logger = logging.getLogger(__name__)

COMMISSION_COLUMN_NAMES = [
    'comision_total', 'comision', 'commission', 'total_commission',
    'Comision', 'Comisión',
]

PERIOD_YYYYMM_LENGTH = 7
PERIOD_YYYYMM_SLICE = 6
ROLE_SAMPLE_LIMIT = 5


class ProcessPlanCommissionHandler(CommandHandler[PlanExecutionResult]):
    def __init__(
        self,
        data_repo: MultiSourceDataRepository,
        calculator: CommissionCalculatorService,
        exporter: Exporter,
        strategy_factory: StrategyFactory
    ):
        self._data_repo = data_repo
        self._calculator = calculator
        self._exporter = exporter
        self._strategy_factory = strategy_factory

    def handle(self, command: ProcessPlanCommissionCommand) -> PlanExecutionResult:
        result, _ = self.handle_with_data(command)
        return result

    def handle_with_data(
        self, command: ProcessPlanCommissionCommand
    ) -> tuple[PlanExecutionResult, pd.DataFrame]:
        start_time = time.time()
        plan = command.plan

        logger.info(f"Processing plan: {plan.full_id} - {plan.name}")

        try:
            target_period = self._resolve_target_period(command.target_period)
            plan.strategy_config.params["target_period"] = target_period
            self._inject_period_into_s3_sources(plan, target_period)

            data = self._get_plan_data(plan, command.dependency_results)

            if data.empty:
                return self._build_empty_data_result(plan, data, start_time)

            result_data = self._execute_strategy(plan, data)

            if result_data.empty:
                return self._build_empty_result(plan, data, result_data, len(data), start_time)

            total_commission = self._sum_commission(result_data)
            self._export_results(result_data, plan)

            return self._build_success_result(plan, result_data, total_commission, start_time)

        except Exception as e:
            return self._build_error_result(plan, command.tenant, e, start_time)

    def _resolve_target_period(self, target_period: str | None) -> str:
        normalized = self._normalize_period(target_period)
        if normalized:
            return normalized
        now = datetime.now()
        return f"{now.year}-{now.month:02d}-01"

    def _execute_strategy(self, plan: Plan, data: pd.DataFrame) -> pd.DataFrame:
        strategy = self._strategy_factory.create_strategy(plan.strategy_config)
        return self._calculator.calculate(plan, data, strategy)

    def _export_results(self, result_data: pd.DataFrame, plan: Plan) -> None:
        self._exporter.export(
            data=result_data,
            output_config=plan.output_config,
            plan_name=plan.name
        )

    def _build_empty_data_result(
        self, plan: Plan, data: pd.DataFrame, start_time: float
    ) -> tuple[PlanExecutionResult, pd.DataFrame]:
        diagnostics = data.attrs.get('diagnostics', {})
        warning_msg = self._build_empty_data_warning(diagnostics)
        logger.warning(f"No data found for plan {plan.full_id}: {warning_msg}")
        return (
            PlanExecutionResult(
                plan_id=plan.id, plan_name=plan.name, success=True,
                records_processed=0, total_commission=0.0,
                warning_message=warning_msg,
                execution_time_seconds=time.time() - start_time
            ),
            pd.DataFrame()
        )

    def _build_empty_result(
        self, plan: Plan, data: pd.DataFrame, result_data: pd.DataFrame,
        input_rows: int, start_time: float
    ) -> tuple[PlanExecutionResult, pd.DataFrame]:
        source_diagnostics = data.attrs.get('diagnostics', {})
        result_diagnostics = result_data.attrs.get('diagnostics', {})
        warning_msg = self._build_empty_result_warning(
            source_diagnostics, result_diagnostics, plan, input_rows
        )
        logger.warning(f"No results after processing for plan {plan.full_id}: {warning_msg}")
        return (
            PlanExecutionResult(
                plan_id=plan.id, plan_name=plan.name, success=True,
                records_processed=0, total_commission=0.0,
                warning_message=warning_msg,
                execution_time_seconds=time.time() - start_time
            ),
            pd.DataFrame()
        )

    def _build_success_result(
        self, plan: Plan, result_data: pd.DataFrame,
        total_commission: float, start_time: float
    ) -> tuple[PlanExecutionResult, pd.DataFrame]:
        execution_time = time.time() - start_time
        logger.info(
            f"Successfully processed plan {plan.full_id}: "
            f"{len(result_data)} records, ${total_commission:,.2f} commission "
            f"in {execution_time:.2f}s"
        )
        return (
            PlanExecutionResult(
                plan_id=plan.id, plan_name=plan.name, success=True,
                records_processed=len(result_data),
                total_commission=total_commission,
                execution_time_seconds=execution_time
            ),
            result_data
        )

    def _build_error_result(
        self, plan: Plan, tenant: Tenant, error: Exception, start_time: float
    ) -> tuple[PlanExecutionResult, pd.DataFrame]:
        execution_time = time.time() - start_time
        error_message = str(error)
        logger.error(
            f"Error processing plan {plan.full_id}: {error_message}",
            exc_info=True
        )
        return (
            PlanExecutionResult(
                plan_id=plan.id, plan_name=plan.name, success=False,
                records_processed=0, total_commission=0.0,
                error_message=error_message,
                execution_time_seconds=execution_time
            ),
            pd.DataFrame()
        )

    def _get_plan_data(self, plan: Plan, dependency_results: dict) -> pd.DataFrame:
        data = self._data_repo.get_data_for_plan(plan)

        if not plan.depends_on or not dependency_results:
            return data

        sources = data.attrs.get('sources', {})
        for dep_plan_id in plan.depends_on:
            if dep_plan_id in dependency_results:
                cached_df = dependency_results[dep_plan_id]
                sources[dep_plan_id] = cached_df
                sources[dep_plan_id.lower()] = cached_df
                logger.info(
                    f"Using cached result for {dep_plan_id}: {len(cached_df)} rows"
                )

        data.attrs['sources'] = sources
        return data

    @staticmethod
    def _sum_commission(df: pd.DataFrame) -> float:
        for name in COMMISSION_COLUMN_NAMES:
            if name in df.columns:
                return float(df[name].sum())
        return 0.0

    @staticmethod
    def _inject_period_into_s3_sources(plan: Plan, target_period: str) -> None:
        period_yyyymm = target_period.replace("-", "")[:PERIOD_YYYYMM_SLICE]

        for source in plan.data_sources.sources:
            if source.source_type != "s3" or "pattern" not in source.config:
                continue
            original_pattern = source.config["pattern"]
            if "*" not in original_pattern:
                continue
            new_pattern = original_pattern.replace("*", period_yyyymm, 1)
            source.config["pattern"] = new_pattern
            logger.info(f"S3 pattern updated: {original_pattern} -> {new_pattern}")

    @staticmethod
    def _normalize_period(period: str | None) -> str | None:
        if not period:
            return None
        if len(period) == PERIOD_YYYYMM_LENGTH:
            return f"{period}-01"
        return period

    @staticmethod
    def _build_empty_data_warning(diagnostics: dict) -> str:
        reasons = []
        reasons.extend(_extract_source_warnings(diagnostics))
        reasons.extend(_extract_merge_warnings(diagnostics))

        if not reasons:
            reasons.append("No data available from configured sources")

        return "; ".join(reasons)

    def _build_empty_result_warning(
        self,
        source_diagnostics: dict,
        result_diagnostics: dict,
        plan: Plan,
        input_rows: int
    ) -> str:
        if input_rows == 0:
            return self._build_empty_data_warning(source_diagnostics)

        merge_info = source_diagnostics.get('merge', {})
        if merge_info.get('plan_data_empty'):
            return "Plan data file is empty or contains no data"

        plan_data_info = source_diagnostics.get('sources', {}).get('plan_data', {})
        if plan_data_info.get('rows', 0) == 0 or plan_data_info.get('error'):
            error_msg = plan_data_info.get('error', 'empty')
            return f"Plan data source is empty or has error: {error_msg}"

        return _build_strategy_warning(result_diagnostics, plan, input_rows)


def _extract_source_warnings(diagnostics: dict) -> list[str]:
    warnings = []
    for source_id, info in diagnostics.get('sources', {}).items():
        if info.get('rows', 0) == 0:
            warnings.append(f"Source '{source_id}' returned 0 rows")
        if info.get('error'):
            warnings.append(f"Source '{source_id}' error: {info['error']}")
    return warnings


def _extract_merge_warnings(diagnostics: dict) -> list[str]:
    merge_info = diagnostics.get('merge', {})
    if merge_info.get('pre_merge_rows', 0) > 0 and merge_info.get('post_merge_rows', 0) == 0:
        return [
            f"Merge produced no matches "
            f"(employees: {merge_info.get('employees_rows', 0)}, "
            f"plan_data: {merge_info.get('plan_data_rows', 0)})"
        ]
    return []


def _build_strategy_warning(result_diagnostics: dict, plan: Plan, input_rows: int) -> str:
    reasons = []
    matched_rows = result_diagnostics.get('matched_rows', 0)
    role_filter = plan.strategy_config.params.get('role_filter', [])

    if role_filter and matched_rows == 0:
        reasons.append(f"Role filter '{role_filter}' matched 0 of {input_rows} employees")
        available_roles = result_diagnostics.get('available_roles', [])
        if available_roles:
            reasons.append(f"Available roles: {available_roles[:ROLE_SAMPLE_LIMIT]}")
    elif matched_rows > 0:
        reasons.append(
            f"All {matched_rows} matching employees have $0 commission (filtered out)"
        )

    if not reasons:
        reasons.append(f"Processing returned 0 records from {input_rows} input rows")

    return "; ".join(reasons)
