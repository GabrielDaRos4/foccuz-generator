import os
import tempfile
from typing import cast
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import yaml

from src.context.commissions.application.commands import (
    CommandBus,
)
from src.context.commissions.application.queries import (
    GetActiveTenantsQuery,
    GetTenantQuery,
    ListTenantPlansQuery,
    QueryBus,
)
from src.context.commissions.domain.aggregates import Plan, Tenant
from src.context.shared.infrastructure.di import DIContainer, bootstrap

COPEC_STRATEGY_MODULE = (
    'src.context.commissions.infrastructure.processing_strategies'
    '.custom.copec.new_client_commission_strategy'
)


@pytest.fixture
def temp_plans_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        test_config = {
            'metadata': {
                'client_id': 'TEST_CLIENT',
                'client_name': 'Test Client S.A.',
                'active': True,
                'gsheet_output': 'test_sheet_id'
            },
            'plans': {
                'PLAN_001': {
                    'active': True,
                    'name': 'Test Plan 001',
                    'description': 'Test plan for integration tests',
                    'data_sources': [{
                        'id': 'test_data',
                        'type': 'csv',
                        'config': {
                            'path': '/tmp/test_data.csv',
                            'separator': ',',
                            'encoding': 'utf-8'
                        }
                    }],
                    'script': {
                        'module': COPEC_STRATEGY_MODULE,
                        'class': 'CopecNewClientCommissionStrategy',
                        'params': {
                            'product_type': 'TCT',
                            'discount_percentage': 0.08,
                            'max_factor': 6.0,
                            'bono_nuevo': 10000,
                            'factor_minimo': 0.5
                        }
                    },
                    'output': {
                        'sheet_id': 'test_sheet_id',
                        'tab_name': 'PLAN_001',
                        'clear_before_write': True
                    }
                },
                'PLAN_002': {
                    'active': True,
                    'name': 'Test Plan 002',
                    'description': 'Second test plan',
                    'data_sources': [{
                        'id': 'test_data',
                        'type': 'csv',
                        'config': {
                            'path': '/tmp/test_data.csv',
                            'separator': ',',
                            'encoding': 'utf-8'
                        }
                    }],
                    'script': {
                        'module': COPEC_STRATEGY_MODULE,
                        'class': 'CopecNewClientCommissionStrategy',
                        'params': {
                            'product_type': 'TAE',
                            'discount_percentage': 0.08,
                            'max_factor': 6.0,
                            'bono_nuevo': 10000,
                            'factor_minimo': 0.5
                        }
                    },
                    'output': {
                        'sheet_id': 'test_sheet_id',
                        'tab_name': 'PLAN_002',
                        'clear_before_write': True
                    }
                },
                'PLAN_INACTIVE': {
                    'active': False,
                    'name': 'Inactive Plan',
                    'description': 'This plan should not be executed',
                    'data_sources': [{
                        'id': 'test_data',
                        'type': 'csv',
                        'config': {'path': '/tmp/test.csv'}
                    }],
                    'script': {
                        'module': 'test',
                        'class': 'Test',
                        'params': {}
                    },
                    'output': {
                        'sheet_id': 'test',
                        'tab_name': 'test',
                        'clear_before_write': True
                    }
                }
            }
        }

        config_path = os.path.join(tmpdir, 'TEST_CLIENT.yaml')
        with open(config_path, 'w') as f:
            yaml.dump(test_config, f)

        yield tmpdir


@pytest.fixture
def sample_sales_data():
    return pd.DataFrame({
        'producto': ['TCT', 'TCT', 'TAE'],
        'ejecutivo': ['0001000001', '0001000001', '0001000001'],
        'rut_ejecutivo': ['12345678', '12345678', '12345678'],
        'dv_ejecutivo': ['9', '9', '9'],
        'rut_cliente': ['11111111', '22222222', '33333333'],
        'dv_cliente': ['1', '2', '3'],
        'volumen': [100.0, 200.0, 150.0],
        'descuento': [50.0, 100.0, 75.0],
        'anio': ['2025', '2025', '2025'],
        'mes': ['10', '10', '10']
    })


class TestCommissionProcessingFlow:

    @patch('src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter')
    @patch('src.context.shared.infrastructure.di.bootstrap._create_source_repositories')
    def test_bootstrap_creates_container(
            self,
            mock_source_repos,
            mock_exporter_class,
            temp_plans_directory
    ):
        mock_source_repos.return_value = {'csv': Mock(), 's3': Mock(), 'json': Mock(), 'buk': Mock(),
                                          'csv_pattern': Mock()}
        mock_exporter_class.return_value = Mock()

        container = bootstrap(plans_directory=temp_plans_directory)

        assert isinstance(container, DIContainer)
        assert container.resolve(CommandBus) is not None
        assert container.resolve(QueryBus) is not None

    @patch('src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter')
    @patch('src.context.shared.infrastructure.di.bootstrap._create_source_repositories')
    def test_query_bus_get_active_tenants(
            self,
            mock_source_repos,
            mock_exporter_class,
            temp_plans_directory
    ):
        mock_source_repos.return_value = {'csv': Mock(), 's3': Mock(), 'json': Mock(), 'buk': Mock(),
                                          'csv_pattern': Mock()}
        mock_exporter_class.return_value = Mock()

        container = bootstrap(plans_directory=temp_plans_directory)
        query_bus = container.resolve(QueryBus)

        tenants = cast(list[Tenant], query_bus.execute(GetActiveTenantsQuery()))

        assert len(tenants) == 1
        assert tenants[0].id == 'TEST_CLIENT'
        assert tenants[0].name == 'Test Client S.A.'

    @patch('src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter')
    @patch('src.context.shared.infrastructure.di.bootstrap._create_source_repositories')
    def test_query_bus_get_tenant_by_id(
            self,
            mock_source_repos,
            mock_exporter_class,
            temp_plans_directory
    ):
        mock_source_repos.return_value = {'csv': Mock(), 's3': Mock(), 'json': Mock(), 'buk': Mock(),
                                          'csv_pattern': Mock()}
        mock_exporter_class.return_value = Mock()

        container = bootstrap(plans_directory=temp_plans_directory)
        query_bus = container.resolve(QueryBus)

        tenant = cast(Tenant | None, query_bus.execute(GetTenantQuery(tenant_id='TEST_CLIENT')))

        assert tenant is not None
        assert tenant.id == 'TEST_CLIENT'
        assert len(tenant.plans) == 3

    @patch('src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter')
    @patch('src.context.shared.infrastructure.di.bootstrap._create_source_repositories')
    def test_query_bus_list_tenant_plans(
            self,
            mock_source_repos,
            mock_exporter_class,
            temp_plans_directory
    ):
        mock_source_repos.return_value = {'csv': Mock(), 's3': Mock(), 'json': Mock(), 'buk': Mock(),
                                          'csv_pattern': Mock()}
        mock_exporter_class.return_value = Mock()

        container = bootstrap(plans_directory=temp_plans_directory)
        query_bus = container.resolve(QueryBus)

        plans = cast(list[Plan], query_bus.execute(ListTenantPlansQuery(tenant_id='TEST_CLIENT')))

        assert len(plans) == 2
        plan_ids = [p.id for p in plans]
        assert 'PLAN_001' in plan_ids
        assert 'PLAN_002' in plan_ids

    @patch('src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter')
    @patch('src.context.shared.infrastructure.di.bootstrap._create_source_repositories')
    def test_get_executable_plans_excludes_inactive(
            self,
            mock_source_repos,
            mock_exporter_class,
            temp_plans_directory
    ):
        mock_source_repos.return_value = {'csv': Mock(), 's3': Mock(), 'json': Mock(), 'buk': Mock(),
                                          'csv_pattern': Mock()}
        mock_exporter_class.return_value = Mock()

        container = bootstrap(plans_directory=temp_plans_directory)
        query_bus = container.resolve(QueryBus)

        tenant = cast(Tenant, query_bus.execute(GetTenantQuery(tenant_id='TEST_CLIENT')))
        executable_plans = tenant.get_executable_plans()

        assert len(executable_plans) == 2
        plan_ids = [p.id for p in executable_plans]
        assert 'PLAN_INACTIVE' not in plan_ids


class TestCLIIntegration:

    @patch('src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter')
    @patch('src.context.shared.infrastructure.di.bootstrap._create_source_repositories')
    def test_cli_list_tenants(
            self,
            mock_source_repos,
            mock_exporter_class,
            temp_plans_directory,
            capsys
    ):
        mock_source_repos.return_value = {'csv': Mock(), 's3': Mock(), 'json': Mock(), 'buk': Mock(),
                                          'csv_pattern': Mock()}
        mock_exporter_class.return_value = Mock()

        from src.adapters.cli.main import main

        with patch('sys.argv', ['main', '--list-tenants', '--plans-dir', temp_plans_directory]):
            result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert 'TEST_CLIENT' in captured.out
        assert 'Test Client S.A.' in captured.out

    @patch('src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter')
    @patch('src.context.shared.infrastructure.di.bootstrap._create_source_repositories')
    def test_cli_list_plans(
            self,
            mock_source_repos,
            mock_exporter_class,
            temp_plans_directory,
            capsys
    ):
        mock_source_repos.return_value = {'csv': Mock(), 's3': Mock(), 'json': Mock(), 'buk': Mock(),
                                          'csv_pattern': Mock()}
        mock_exporter_class.return_value = Mock()

        from src.adapters.cli.main import main

        with patch('sys.argv', [
            'main',
            '--list-plans',
            '--tenant', 'TEST_CLIENT',
            '--plans-dir', temp_plans_directory
        ]):
            result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert 'PLAN_001' in captured.out
        assert 'PLAN_002' in captured.out

    @patch('src.context.commissions.infrastructure.exporters.gsheet_exporter.GSheetExporter')
    @patch('src.context.shared.infrastructure.di.bootstrap._create_source_repositories')
    def test_cli_invalid_tenant_returns_error(
            self,
            mock_source_repos,
            mock_exporter_class,
            temp_plans_directory,
            capsys
    ):
        mock_source_repos.return_value = {'csv': Mock(), 's3': Mock(), 'json': Mock(), 'buk': Mock(),
                                          'csv_pattern': Mock()}
        mock_exporter_class.return_value = Mock()

        from src.adapters.cli.main import main

        with patch('sys.argv', [
            'main',
            '--tenant', 'INVALID_TENANT',
            '--plans-dir', temp_plans_directory
        ]):
            result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert 'Error' in captured.out or 'not found' in captured.out.lower()
