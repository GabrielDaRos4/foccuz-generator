import argparse
import logging
import sys
from typing import cast

from dotenv import load_dotenv

load_dotenv()

from src.context.commissions.application.commands import (
    CommandBus,
    ProcessAllTenantsCommand,
    ProcessTenantCommissionsCommand,
)
from src.context.commissions.application.dto import BatchExecutionResult, TenantExecutionResult
from src.context.commissions.application.queries import (
    GetActiveTenantsQuery,
    GetTenantQuery,
    QueryBus,
)
from src.context.commissions.domain.aggregates import Tenant
from src.context.shared.infrastructure.di import bootstrap

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('commissions.log')
    ]
)

logger = logging.getLogger(__name__)


def print_execution_result(result):
    print("\n" + "="*80)
    print(f"COMMISSION PROCESSING REPORT - Tenant: {result.tenant_name} ({result.tenant_id})")
    print("="*80)
    print(f"Total Plans: {result.total_plans}")
    print(f"Successful: {result.successful_plans}")
    print(f"Failed: {result.failed_plans}")
    print(f"Success Rate: {result.success_rate:.1f}%")
    print(f"Execution Time: {result.execution_time_seconds:.2f}s")
    print("\n" + "-"*80)
    print("PLAN DETAILS:")
    print("-"*80)

    for plan_result in result.plan_results:
        status = "SUCCESS" if plan_result.success else "FAILED"
        print(f"\n{status} - {plan_result.plan_name} ({plan_result.plan_id})")
        print(f"  Records Processed: {plan_result.records_processed}")
        print(f"  Total Commission: ${plan_result.total_commission:,.2f}")
        print(f"  Execution Time: {plan_result.execution_time_seconds:.2f}s")

        if plan_result.error_message:
            print(f"  Error: {plan_result.error_message}")

        if plan_result.warning_message:
            print(f"  Warning: {plan_result.warning_message}")

    print("\n" + "="*80 + "\n")


def print_batch_execution_result(result):
    print("\n" + "="*80)
    print("BATCH COMMISSION PROCESSING REPORT - ALL ACTIVE TENANTS")
    print("="*80)
    print(f"Total Tenants: {result.total_tenants}")
    print(f"Successful Tenants: {result.successful_tenants}")
    print(f"Failed Tenants: {result.failed_tenants}")
    print(f"Tenant Success Rate: {result.tenant_success_rate:.1f}%")
    print("-"*40)
    print(f"Total Plans: {result.total_plans}")
    print(f"Successful Plans: {result.successful_plans}")
    print(f"Failed Plans: {result.failed_plans}")
    print(f"Plan Success Rate: {result.plan_success_rate:.1f}%")
    print("-"*40)
    print(f"Total Execution Time: {result.execution_time_seconds:.2f}s")

    print("\n" + "-"*80)
    print("TENANT DETAILS:")
    print("-"*80)

    for tenant_result in result.tenant_results:
        tenant_status = "OK" if tenant_result.failed_plans == 0 else "FAIL"
        print(f"\n{tenant_status} {tenant_result.tenant_name} ({tenant_result.tenant_id})")
        print(f"  Plans: {tenant_result.successful_plans}/{tenant_result.total_plans} successful")
        print(f"  Execution Time: {tenant_result.execution_time_seconds:.2f}s")

        for plan_result in tenant_result.plan_results:
            plan_status = "OK" if plan_result.success else "FAIL"
            print(f"    {plan_status} {plan_result.plan_name}: ${plan_result.total_commission:,.2f}")
            if plan_result.error_message:
                print(f"      Error: {plan_result.error_message}")

    print("\n" + "="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(description='Process commissions for tenants')

    parser.add_argument(
        '--tenant',
        type=str,
        help='Tenant ID to process'
    )

    parser.add_argument(
        '--plans',
        nargs='+',
        help='Specific plan IDs to process (optional)'
    )

    parser.add_argument(
        '--period',
        type=str,
        help='Target period in YYYY-MM format (e.g., 2024-11)'
    )

    parser.add_argument(
        '--list-tenants',
        action='store_true',
        help='List all available tenants'
    )

    parser.add_argument(
        '--list-plans',
        action='store_true',
        help='List all plans for a tenant (requires --tenant)'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all plans for all active tenants'
    )

    parser.add_argument(
        '--plans-dir',
        type=str,
        help='Path to plans directory (default: config/plans)'
    )

    parser.add_argument(
        '--credentials',
        type=str,
        help='Path to Google credentials JSON file'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        container = bootstrap(
            plans_directory=args.plans_dir,
            credentials_path=args.credentials
        )

        command_bus = container.resolve(CommandBus)
        query_bus = container.resolve(QueryBus)

        if args.list_tenants:
            tenants = cast(list[Tenant], query_bus.execute(GetActiveTenantsQuery()))
            print("\nAvailable Tenants:")
            print("-" * 80)
            for tenant in tenants:
                status = "ACTIVE" if tenant.active else "INACTIVE"
                print(f"{tenant.id:20} {tenant.name:30} [{status}] {len(tenant.plans)} plans")
            print()
            return 0

        if args.list_plans:
            if not args.tenant:
                print("Error: --tenant is required when using --list-plans")
                return 1

            tenant = cast(Tenant | None, query_bus.execute(GetTenantQuery(tenant_id=args.tenant)))
            if not tenant:
                print(f"Error: Tenant '{args.tenant}' not found")
                return 1

            print(f"\nPlans for Tenant: {tenant.name} ({tenant.id})")
            print("-" * 80)
            for plan in tenant.plans:
                status = "ACTIVE" if plan.active else "INACTIVE"
                executable = "[OK]" if plan.is_executable() else "[--]"
                print(f"{executable} {plan.id:15} {plan.name:30} [{status}]")
            print()
            return 0

        if getattr(args, 'all', False):
            target_period = args.period if args.period else None
            result = cast(
                BatchExecutionResult,
                command_bus.execute(ProcessAllTenantsCommand(target_period=target_period))
            )
            print_batch_execution_result(result)
            return 0 if result.failed_plans == 0 else 1

        if args.tenant:
            target_period = args.period if args.period else None
            result = cast(
                TenantExecutionResult,
                command_bus.execute(
                    ProcessTenantCommissionsCommand(
                        tenant_id=args.tenant,
                        plan_ids=args.plans,
                        target_period=target_period
                    )
                )
            )
            print_execution_result(result)
            return 0 if result.failed_plans == 0 else 1

        parser.print_help()
        return 0

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        print(f"\nError: {str(e)}\n")
        return 1


if __name__ == '__main__':
    sys.exit(main())
