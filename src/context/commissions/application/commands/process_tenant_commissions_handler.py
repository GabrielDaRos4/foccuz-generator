import logging
import time

from src.context.commissions.application.commands.process_plan_commission_command import (
    ProcessPlanCommissionCommand,
)
from src.context.commissions.application.commands.process_plan_commission_handler import (
    ProcessPlanCommissionHandler,
)
from src.context.commissions.application.commands.process_tenant_commissions_command import (
    ProcessTenantCommissionsCommand,
)
from src.context.commissions.application.dto import TenantExecutionResult
from src.context.commissions.domain.aggregates import Plan, Tenant
from src.context.commissions.domain.exceptions import InvalidTenantError
from src.context.commissions.domain.repositories import MultiSourceDataRepository, TenantRepository
from src.context.shared.domain.cqrs import CommandHandler

logger = logging.getLogger(__name__)


class ProcessTenantCommissionsHandler(CommandHandler[TenantExecutionResult]):
    def __init__(
        self,
        tenant_repo: TenantRepository,
        plan_handler: ProcessPlanCommissionHandler,
        data_repo: MultiSourceDataRepository | None = None
    ):
        self._tenant_repo = tenant_repo
        self._plan_handler = plan_handler
        self._data_repo = data_repo

    def handle(self, command: ProcessTenantCommissionsCommand) -> TenantExecutionResult:
        start_time = time.time()
        tenant_id = command.tenant_id
        plan_ids = command.plan_ids
        target_period = command.target_period

        logger.info(f"Starting commission processing for tenant: {tenant_id}")

        self._start_cache_session(tenant_id)

        tenant = self._tenant_repo.get_by_id(tenant_id)
        if not tenant:
            raise InvalidTenantError(f"Tenant not found: {tenant_id}")

        if not tenant.active:
            raise InvalidTenantError(f"Tenant {tenant_id} is not active")

        plans = self._get_plans_to_process(tenant, plan_ids)

        if not plans:
            logger.warning(f"No executable plans found for tenant {tenant_id}")
            return TenantExecutionResult(
                tenant_id=tenant.id,
                tenant_name=tenant.name,
                total_plans=0,
                successful_plans=0,
                failed_plans=0,
                plan_results=[],
                execution_time_seconds=time.time() - start_time
            )

        ordered_plans = self._resolve_dependencies(plans, tenant)
        logger.info(f"Processing {len(ordered_plans)} plans for tenant {tenant_id}")

        plan_results = []
        executed_plan_ids = set()
        plan_result_cache = {}

        for plan in ordered_plans:
            if plan.id in executed_plan_ids:  # pragma: no cover
                continue

            plan_command = ProcessPlanCommissionCommand(
                tenant=tenant,
                plan=plan,
                target_period=target_period,
                dependency_results=plan_result_cache
            )
            result, result_data = self._plan_handler.handle_with_data(plan_command)
            plan_results.append(result)
            executed_plan_ids.add(plan.id)

            if result_data is not None and not result_data.empty:
                plan_result_cache[plan.id] = result_data
                logger.info(f"Cached result for {plan.id}: {len(result_data)} rows")

        successful = sum(1 for r in plan_results if r.success)
        failed = len(plan_results) - successful
        execution_time = time.time() - start_time

        result = TenantExecutionResult(
            tenant_id=tenant.id,
            tenant_name=tenant.name,
            total_plans=len(plans),
            successful_plans=successful,
            failed_plans=failed,
            plan_results=plan_results,
            execution_time_seconds=execution_time
        )

        self._end_cache_session(tenant_id)

        logger.info(
            f"Completed tenant {tenant_id}: "
            f"{successful}/{len(plans)} plans successful in {execution_time:.2f}s"
        )

        return result

    def _start_cache_session(self, tenant_id: str) -> None:
        if self._data_repo and hasattr(self._data_repo, 'start_cache_session'):
            self._data_repo.start_cache_session(session_id=tenant_id)

    def _end_cache_session(self, tenant_id: str) -> None:
        if self._data_repo and hasattr(self._data_repo, 'end_cache_session'):
            cache_stats = self._data_repo.end_cache_session()
            logger.info(
                f"Tenant {tenant_id} cache: {cache_stats['hits']} hits, "
                f"{cache_stats['misses']} misses, {cache_stats['hit_rate']:.1%} hit rate"
            )

    @staticmethod
    def _get_plans_to_process(tenant: Tenant, plan_ids: list[str] | None) -> list[Plan]:
        executable_plans = tenant.get_executable_plans()
        if plan_ids:
            return [p for p in executable_plans if p.id in plan_ids]
        return executable_plans

    @staticmethod
    def _resolve_dependencies(plans: list[Plan], tenant: Tenant) -> list[Plan]:
        all_plans = {p.id: p for p in tenant.get_executable_plans()}
        requested_plan_ids = {p.id for p in plans}

        ordered = []
        visited = set()
        in_stack = set()

        def visit(plan_id: str):
            if plan_id in visited:
                return
            if plan_id in in_stack:
                logger.warning(f"Circular dependency detected for plan {plan_id}")
                return

            in_stack.add(plan_id)

            plan = all_plans.get(plan_id)
            if plan:
                for dep_id in plan.depends_on:
                    if dep_id in all_plans:
                        visit(dep_id)
                    else:
                        logger.warning(f"Dependency {dep_id} not found for plan {plan_id}")

                visited.add(plan_id)
                ordered.append(plan)

            in_stack.discard(plan_id)

        for plan in plans:
            visit(plan.id)

        dep_count = len(ordered) - len(requested_plan_ids)
        if dep_count > 0:
            logger.info(f"Added {dep_count} dependency plans to execution order")

        return ordered
