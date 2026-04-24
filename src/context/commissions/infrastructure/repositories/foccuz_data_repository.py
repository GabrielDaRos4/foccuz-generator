import logging

import pandas as pd

from src.context.commissions.domain.repositories import DataRepository
from src.context.commissions.domain.value_objects import DataSourceConfig

from .foccuz_connection_config import FoccuzConnectionConfig

logger = logging.getLogger(__name__)


class FoccuzDataRepository(DataRepository):

    SUPPORTED_TABLES = {
        'deals': 'DealV2',
        'sales_profiles': 'SalesProfileV2',
        'sales_reps': 'SalesRep',
        'users': 'UserV2',
        'custom_fields': 'CustomFieldsV2',
        'deal_custom_fields': 'DealCustomFieldMappingV2',
    }

    def __init__(self):
        self._connection = None

    def get_data_for_plan(self, source: DataSourceConfig) -> pd.DataFrame:
        config = source.config
        conn_config = FoccuzConnectionConfig.from_config(config)

        if not conn_config.tenant_id:
            raise ValueError("tenant_id is required for Foccuz repository")

        table_key = config.get('table', 'deals')
        query = config.get('query')

        if query:
            return self._execute_custom_query(conn_config, query)

        return self._fetch_table_data(conn_config, table_key, config)

    @staticmethod
    def _get_connection(conn_config: FoccuzConnectionConfig):
        try:
            import psycopg2
        except ImportError as err:
            raise ImportError(
                "psycopg2 is required for FoccuzDataRepository. "
                "Install it with: pip install psycopg2-binary"
            ) from err

        return psycopg2.connect(
            host=conn_config.host,
            port=conn_config.port,
            database=conn_config.database,
            user=conn_config.user,
            password=conn_config.password,
            options=f"-c app.current_tenant={conn_config.tenant_id}"
        )

    def _execute_custom_query(
        self,
        conn_config: FoccuzConnectionConfig,
        query: str
    ) -> pd.DataFrame:
        logger.info(f"Executing custom query for tenant: {conn_config.tenant_id}")

        conn = self._get_connection(conn_config)
        try:
            df = pd.read_sql_query(query, conn)
            logger.info(f"Query returned {len(df)} rows")
            return df
        finally:
            conn.close()

    def _fetch_table_data(
        self,
        conn_config: FoccuzConnectionConfig,
        table_key: str,
        config: dict
    ) -> pd.DataFrame:
        table_name = self.SUPPORTED_TABLES.get(table_key)
        if not table_name:
            raise ValueError(f"Unknown table key: {table_key}. Supported: {list(self.SUPPORTED_TABLES.keys())}")

        columns = config.get('columns', ['*'])
        filters = config.get('filters', {})
        limit = config.get('limit')

        query = self._build_select_query(table_name, columns, conn_config.tenant_id, filters, limit)

        logger.info(f"Fetching data from {table_name} for tenant: {conn_config.tenant_id}")

        conn = self._get_connection(conn_config)
        try:
            df = pd.read_sql_query(query, conn)
            logger.info(f"Fetched {len(df)} rows from {table_name}")
            return df
        finally:
            conn.close()

    @staticmethod
    def _build_select_query(
        table_name: str,
        columns: list[str],
        tenant_id: str,
        filters: dict,
        limit: int | None
    ) -> str:
        cols = ', '.join(f'"{c}"' if c != '*' else c for c in columns)
        query = f'SELECT {cols} FROM "{table_name}" WHERE "tenantId" = %s'

        params = [tenant_id]

        for col, value in filters.items():
            if isinstance(value, list):
                placeholders = ', '.join(['%s'] * len(value))
                query += f' AND "{col}" IN ({placeholders})'
                params.extend(value)
            elif value is None:
                query += f' AND "{col}" IS NULL'
            else:
                query += f' AND "{col}" = %s'
                params.append(value)

        if limit:
            query += f' LIMIT {int(limit)}'

        return query % tuple(f"'{p}'" if isinstance(p, str) else p for p in params)

    def fetch_deals_with_details(
        self,
        tenant_id: str,
        won_only: bool = True,
        start_date: str = None,
        end_date: str = None
    ) -> pd.DataFrame:
        conn_config = FoccuzConnectionConfig.from_config({'tenant_id': tenant_id})

        query = f"""
            SELECT
                d.id as deal_id,
                d.uuid as deal_uuid,
                d."externalId" as external_id,
                d.title,
                d."isWon" as is_won,
                d."closeDate" as close_date,
                d."createdAt" as created_at,
                sr.id as sales_rep_id,
                sr.name as sales_rep_name,
                sr.email as sales_rep_email,
                sr."job_title" as job_title
            FROM "DealV2" d
            LEFT JOIN "SalesRep" sr ON d."ownerId" = sr.id
            WHERE d."tenantId" = '{tenant_id}'
        """

        if won_only:
            query += ' AND d."isWon" = true'

        if start_date:
            query += f" AND d.\"closeDate\" >= '{start_date}'"

        if end_date:
            query += f" AND d.\"closeDate\" <= '{end_date}'"

        query += ' ORDER BY d."closeDate" DESC'

        return self._execute_custom_query(conn_config, query)

    def fetch_deal_custom_fields(self, tenant_id: str, deal_ids: list[int] = None) -> pd.DataFrame:
        conn_config = FoccuzConnectionConfig.from_config({'tenant_id': tenant_id})

        query = f"""
            SELECT
                dcm."dealId" as deal_id,
                cf.name as field_name,
                dcm.value as field_value
            FROM "DealCustomFieldMappingV2" dcm
            JOIN "CustomFieldsV2" cf ON dcm."customFieldId" = cf.id
            WHERE dcm."tenantId" = '{tenant_id}'
        """

        if deal_ids:
            deal_ids_str = ', '.join(str(d) for d in deal_ids)
            query += f' AND dcm."dealId" IN ({deal_ids_str})'

        return self._execute_custom_query(conn_config, query)

    def fetch_sales_reps(self, tenant_id: str) -> pd.DataFrame:
        conn_config = FoccuzConnectionConfig.from_config({'tenant_id': tenant_id})

        query = f"""
            SELECT
                sr.id,
                sr.name,
                sr.email,
                sr."job_title",
                sr.rut,
                u.id as user_id,
                u.email as user_email
            FROM "SalesRep" sr
            LEFT JOIN "UserV2" u ON sr."userId" = u.id
            WHERE sr."tenantId" = '{tenant_id}'
            ORDER BY sr.name
        """

        return self._execute_custom_query(conn_config, query)

    def fetch_deals_with_custom_field(
        self,
        tenant_id: str,
        field_name: str,
        field_value: str = None
    ) -> pd.DataFrame:
        conn_config = FoccuzConnectionConfig.from_config({'tenant_id': tenant_id})

        query = f"""
            SELECT
                d.id as deal_id,
                d.title,
                d."isWon" as is_won,
                d."closeDate" as close_date,
                sr.name as sales_rep_name,
                cf.name as field_name,
                dcm.value as field_value
            FROM "DealV2" d
            LEFT JOIN "SalesRep" sr ON d."ownerId" = sr.id
            LEFT JOIN "DealCustomFieldMappingV2" dcm ON d.id = dcm."dealId"
            LEFT JOIN "CustomFieldsV2" cf ON dcm."customFieldId" = cf.id
            WHERE d."tenantId" = '{tenant_id}'
            AND cf.name = '{field_name}'
        """

        if field_value:
            query += f" AND dcm.value = '{field_value}'"

        return self._execute_custom_query(conn_config, query)
