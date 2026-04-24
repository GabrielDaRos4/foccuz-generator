from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.context.commissions.domain.value_objects import DataSourceConfig
from src.context.commissions.infrastructure.repositories.foccuz_connection_config import (
    FoccuzConnectionConfig,
)
from src.context.commissions.infrastructure.repositories.foccuz_data_repository import (
    FoccuzDataRepository,
)


class TestFoccuzConnectionConfig:

    def test_should_parse_connection_string(self):
        config = {
            'connection_string': 'postgresql://user:pass@localhost:5432/mydb',
            'tenant_id': 'test_tenant'
        }

        result = FoccuzConnectionConfig.from_config(config)

        assert result.host == 'localhost'
        assert result.port == 5432
        assert result.database == 'mydb'
        assert result.user == 'user'
        assert result.password == 'pass'
        assert result.tenant_id == 'test_tenant'

    def test_should_use_config_values_over_connection_string(self):
        config = {
            'connection_string': 'postgresql://user:pass@localhost:5432/mydb',
            'host': 'custom_host',
            'tenant_id': 'test_tenant'
        }

        result = FoccuzConnectionConfig.from_config(config)

        assert result.host == 'custom_host'

    def test_should_handle_missing_connection_string(self):
        config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass',
            'tenant_id': 'test_tenant'
        }

        result = FoccuzConnectionConfig.from_config(config)

        assert result.tenant_id == 'test_tenant'


class TestFoccuzDataRepository:

    @pytest.fixture
    def repository(self):
        return FoccuzDataRepository()

    @pytest.fixture
    def mock_connection(self):
        return MagicMock()

    @pytest.fixture
    def source_config(self):
        return DataSourceConfig(
            source_id='test_source',
            source_type='foccuz',
            config={
                'tenant_id': 'gocar',
                'table': 'deals'
            }
        )


class TestGetDataForPlan(TestFoccuzDataRepository):

    def test_should_raise_error_when_tenant_id_missing(self, repository):
        source = DataSourceConfig(
            source_id='test',
            source_type='foccuz',
            config={'table': 'deals'}
        )

        with pytest.raises(ValueError, match="tenant_id is required"):
            repository.get_data_for_plan(source)

    @patch('src.context.commissions.infrastructure.repositories.foccuz_data_repository.FoccuzDataRepository._get_connection')
    def test_should_execute_custom_query_when_provided(self, mock_get_conn, repository):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        with patch('pandas.read_sql_query') as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame({'id': [1, 2, 3]})

            source = DataSourceConfig(
                source_id='test',
                source_type='foccuz',
                config={
                    'tenant_id': 'gocar',
                    'query': 'SELECT * FROM "DealV2"'
                }
            )

            result = repository.get_data_for_plan(source)

            assert len(result) == 3
            mock_read_sql.assert_called_once()


class TestSupportedTables(TestFoccuzDataRepository):

    def test_should_have_deals_table(self, repository):
        assert 'deals' in repository.SUPPORTED_TABLES
        assert repository.SUPPORTED_TABLES['deals'] == 'DealV2'

    def test_should_have_sales_profiles_table(self, repository):
        assert 'sales_profiles' in repository.SUPPORTED_TABLES
        assert repository.SUPPORTED_TABLES['sales_profiles'] == 'SalesProfileV2'

    def test_should_have_sales_reps_table(self, repository):
        assert 'sales_reps' in repository.SUPPORTED_TABLES
        assert repository.SUPPORTED_TABLES['sales_reps'] == 'SalesRep'

    def test_should_have_custom_fields_table(self, repository):
        assert 'custom_fields' in repository.SUPPORTED_TABLES
        assert repository.SUPPORTED_TABLES['custom_fields'] == 'CustomFieldsV2'

    def test_should_have_deal_custom_fields_table(self, repository):
        assert 'deal_custom_fields' in repository.SUPPORTED_TABLES
        assert repository.SUPPORTED_TABLES['deal_custom_fields'] == 'DealCustomFieldMappingV2'


class TestBuildSelectQuery(TestFoccuzDataRepository):

    def test_should_build_basic_select_query(self, repository):
        result = repository._build_select_query(
            table_name='DealV2',
            columns=['*'],
            tenant_id='gocar',
            filters={},
            limit=None
        )

        assert 'SELECT *' in result
        assert '"DealV2"' in result
        assert '"tenantId"' in result
        assert "'gocar'" in result

    def test_should_build_query_with_specific_columns(self, repository):
        result = repository._build_select_query(
            table_name='DealV2',
            columns=['id', 'title', 'isWon'],
            tenant_id='gocar',
            filters={},
            limit=None
        )

        assert '"id"' in result
        assert '"title"' in result
        assert '"isWon"' in result

    def test_should_build_query_with_limit(self, repository):
        result = repository._build_select_query(
            table_name='DealV2',
            columns=['*'],
            tenant_id='gocar',
            filters={},
            limit=100
        )

        assert 'LIMIT 100' in result

    def test_should_build_query_with_filters(self, repository):
        result = repository._build_select_query(
            table_name='DealV2',
            columns=['*'],
            tenant_id='gocar',
            filters={'isWon': True},
            limit=None
        )

        assert '"isWon"' in result
