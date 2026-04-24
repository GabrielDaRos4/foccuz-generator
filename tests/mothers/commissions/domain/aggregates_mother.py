from src.context.commissions.domain.aggregates import Plan, Tenant
from src.context.commissions.domain.value_objects import ValidityPeriod

from .value_objects_mother import (
    DataSourceCollectionMother,
    OutputConfigMother,
    StrategyConfigMother,
    ValidityPeriodMother,
)


class PlanMother:

    @staticmethod
    def active(
        plan_id: str = "PLAN_001",
        name: str = "Test Plan",
        tenant_id: str = "TEST_TENANT"
    ) -> Plan:
        return Plan(
            id=plan_id,
            name=name,
            tenant_id=tenant_id,
            active=True,
            data_sources=DataSourceCollectionMother.single_csv(),
            output_config=OutputConfigMother.default(),
            strategy_config=StrategyConfigMother.tiered_commission()
        )

    @staticmethod
    def inactive(
        plan_id: str = "PLAN_002",
        name: str = "Inactive Plan",
        tenant_id: str = "TEST_TENANT"
    ) -> Plan:
        return Plan(
            id=plan_id,
            name=name,
            tenant_id=tenant_id,
            active=False,
            data_sources=DataSourceCollectionMother.single_csv(),
            output_config=OutputConfigMother.default(),
            strategy_config=StrategyConfigMother.tiered_commission()
        )

    @staticmethod
    def expired(
        plan_id: str = "PLAN_003",
        name: str = "Expired Plan",
        tenant_id: str = "TEST_TENANT"
    ) -> Plan:
        return Plan(
            id=plan_id,
            name=name,
            tenant_id=tenant_id,
            active=True,
            data_sources=DataSourceCollectionMother.single_csv(),
            output_config=OutputConfigMother.default(),
            strategy_config=StrategyConfigMother.tiered_commission(),
            validity_period=ValidityPeriodMother.expired()
        )

    @staticmethod
    def copec_tct(tenant_id: str = "COPEC") -> Plan:
        return Plan(
            id="PLAN_800",
            name="PLAN TCT - Clientes Nuevos",
            tenant_id=tenant_id,
            active=True,
            data_sources=DataSourceCollectionMother.multi_source_with_join(),
            output_config=OutputConfigMother.default(tab_name="PLAN_800"),
            strategy_config=StrategyConfigMother.copec_new_client("TCT")
        )

    @staticmethod
    def with_multi_source(
        plan_id: str = "PLAN_MULTI",
        name: str = "Multi Source Plan",
        tenant_id: str = "TEST_TENANT"
    ) -> Plan:
        return Plan(
            id=plan_id,
            name=name,
            tenant_id=tenant_id,
            active=True,
            data_sources=DataSourceCollectionMother.multi_source_with_join(),
            output_config=OutputConfigMother.default(),
            strategy_config=StrategyConfigMother.tiered_commission()
        )

    @staticmethod
    def custom(
        plan_id: str,
        name: str,
        tenant_id: str,
        active: bool = True,
        validity_period: ValidityPeriod = None
    ) -> Plan:
        return Plan(
            id=plan_id,
            name=name,
            tenant_id=tenant_id,
            active=active,
            data_sources=DataSourceCollectionMother.single_csv(),
            output_config=OutputConfigMother.default(),
            strategy_config=StrategyConfigMother.tiered_commission(),
            validity_period=validity_period or ValidityPeriodMother.always_valid()
        )


class TenantMother:

    @staticmethod
    def active(
        tenant_id: str = "TEST_TENANT",
        name: str = "Test Tenant S.A.",
        gsheet_id: str = "sheet123"
    ) -> Tenant:
        return Tenant(
            id=tenant_id,
            name=name,
            gsheet_id=gsheet_id,
            active=True
        )

    @staticmethod
    def inactive(
        tenant_id: str = "INACTIVE_TENANT",
        name: str = "Inactive Tenant S.A.",
        gsheet_id: str = "sheet456"
    ) -> Tenant:
        return Tenant(
            id=tenant_id,
            name=name,
            gsheet_id=gsheet_id,
            active=False
        )

    @staticmethod
    def with_plans(
        tenant_id: str = "TEST_TENANT",
        name: str = "Test Tenant S.A.",
        plans_count: int = 2
    ) -> Tenant:
        tenant = Tenant(
            id=tenant_id,
            name=name,
            gsheet_id="sheet123",
            active=True
        )
        for i in range(plans_count):
            plan = PlanMother.active(
                plan_id=f"PLAN_{i+1:03d}",
                name=f"Plan {i+1}",
                tenant_id=tenant_id
            )
            tenant.add_plan(plan)
        return tenant

    @staticmethod
    def copec() -> Tenant:
        tenant = Tenant(
            id="COPEC",
            name="Copec S.A.",
            gsheet_id="copec_sheet_id",
            active=True
        )
        tenant.add_plan(PlanMother.copec_tct("COPEC"))
        return tenant

    @staticmethod
    def custom(
        tenant_id: str,
        name: str,
        gsheet_id: str = "sheet123",
        active: bool = True
    ) -> Tenant:
        return Tenant(
            id=tenant_id,
            name=name,
            gsheet_id=gsheet_id,
            active=active
        )
