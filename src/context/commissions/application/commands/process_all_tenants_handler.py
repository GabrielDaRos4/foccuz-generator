import logging
import time

from src.context.commissions.application.commands.process_all_tenants_command import (
    ProcessAllTenantsCommand,
)
from src.context.commissions.application.commands.process_tenant_commissions_command import (
    ProcessTenantCommissionsCommand,
)
from src.context.commissions.application.commands.process_tenant_commissions_handler import (
    ProcessTenantCommissionsHandler,
)
from src.context.commissions.application.dto import BatchExecutionResult, TenantExecutionResult
from src.context.commissions.domain.repositories import TenantRepository
from src.context.shared.domain.cqrs import CommandHandler

logger = logging.getLogger(__name__)


class ProcessAllTenantsHandler(CommandHandler[BatchExecutionResult]):
    def __init__(
        self,
        tenant_repo: TenantRepository,
        tenant_handler: ProcessTenantCommissionsHandler
    ):
        self._tenant_repo = tenant_repo
        self._tenant_handler = tenant_handler

    def handle(self, command: ProcessAllTenantsCommand) -> BatchExecutionResult:
        start_time = time.time()

        logger.info("Starting batch processing for all active tenants")

        active_tenants = self._tenant_repo.get_active_tenants()

        if not active_tenants:
            logger.warning("No active tenants found")
            return BatchExecutionResult(
                total_tenants=0,
                successful_tenants=0,
                failed_tenants=0,
                total_plans=0,
                successful_plans=0,
                failed_plans=0,
                tenant_results=[],
                execution_time_seconds=time.time() - start_time
            )

        logger.info(f"Processing {len(active_tenants)} active tenants")

        tenant_results = []
        for tenant in active_tenants:
            try:
                tenant_command = ProcessTenantCommissionsCommand(
                    tenant_id=tenant.id,
                    target_period=command.target_period
                )
                result = self._tenant_handler.handle(tenant_command)
                tenant_results.append(result)
            except Exception as e:
                logger.error(f"Error processing tenant {tenant.id}: {str(e)}")
                tenant_results.append(TenantExecutionResult(
                    tenant_id=tenant.id,
                    tenant_name=tenant.name,
                    total_plans=len(tenant.get_executable_plans()),
                    successful_plans=0,
                    failed_plans=len(tenant.get_executable_plans()),
                    plan_results=[],
                    execution_time_seconds=0.0
                ))

        total_plans = sum(r.total_plans for r in tenant_results)
        successful_plans = sum(r.successful_plans for r in tenant_results)
        failed_plans = sum(r.failed_plans for r in tenant_results)

        successful_tenants = sum(
            1 for r in tenant_results
            if r.failed_plans == 0 and r.total_plans > 0
        )
        failed_tenants = len(tenant_results) - successful_tenants

        execution_time = time.time() - start_time

        result = BatchExecutionResult(
            total_tenants=len(active_tenants),
            successful_tenants=successful_tenants,
            failed_tenants=failed_tenants,
            total_plans=total_plans,
            successful_plans=successful_plans,
            failed_plans=failed_plans,
            tenant_results=tenant_results,
            execution_time_seconds=execution_time
        )

        logger.info(
            f"Batch processing completed: "
            f"{successful_tenants}/{len(active_tenants)} tenants, "
            f"{successful_plans}/{total_plans} plans in {execution_time:.2f}s"
        )

        return result
