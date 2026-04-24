from unittest.mock import MagicMock

import pytest

from src.context.commissions.application.commands import (
    ProcessAllTenantsCommand,
    ProcessPlanCommissionCommand,
    ProcessTenantCommissionsCommand,
)
from src.context.commissions.application.queries import (
    GetActiveTenantsQuery,
    GetTenantQuery,
    ListTenantPlansQuery,
)
from src.context.commissions.domain.ports import Exporter, StrategyFactory
from src.context.commissions.domain.repositories import (
    MultiSourceDataRepository,
    TenantRepository,
)
from src.context.commissions.domain.services import CommissionCalculatorService
from src.context.commissions.infrastructure.config.cqrs_config import (
    configure_command_bus,
    configure_query_bus,
)
from src.context.shared.infrastructure.di import DIContainer


class TestConfigureCommandBus:

    @pytest.fixture
    def container(self):
        container = DIContainer()
        container.register_transient(MultiSourceDataRepository, lambda: MagicMock())
        container.register_transient(CommissionCalculatorService, lambda: MagicMock())
        container.register_transient(Exporter, lambda: MagicMock())
        container.register_transient(StrategyFactory, lambda: MagicMock())
        container.register_transient(TenantRepository, lambda: MagicMock())
        return container

    def test_creates_command_bus(self, container):
        bus = configure_command_bus(container)

        assert bus is not None

    def test_registers_process_plan_command(self, container):
        bus = configure_command_bus(container)

        assert ProcessPlanCommissionCommand in bus._handlers

    def test_registers_process_tenant_command(self, container):
        bus = configure_command_bus(container)

        assert ProcessTenantCommissionsCommand in bus._handlers

    def test_registers_process_all_tenants_command(self, container):
        bus = configure_command_bus(container)

        assert ProcessAllTenantsCommand in bus._handlers


class TestConfigureQueryBus:

    @pytest.fixture
    def container(self):
        container = DIContainer()
        container.register_transient(TenantRepository, lambda: MagicMock())
        return container

    def test_creates_query_bus(self, container):
        bus = configure_query_bus(container)

        assert bus is not None

    def test_registers_get_tenant_query(self, container):
        bus = configure_query_bus(container)

        assert GetTenantQuery in bus._handlers

    def test_registers_get_active_tenants_query(self, container):
        bus = configure_query_bus(container)

        assert GetActiveTenantsQuery in bus._handlers

    def test_registers_list_tenant_plans_query(self, container):
        bus = configure_query_bus(container)

        assert ListTenantPlansQuery in bus._handlers
