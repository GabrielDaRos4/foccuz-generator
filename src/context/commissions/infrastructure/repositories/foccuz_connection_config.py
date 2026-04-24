import os
from dataclasses import dataclass

DEFAULT_CONNECTION_STRING = (
    "postgresql://foccuz_reader:tLbbH1sqJsqMxmIq9SCR2LRHKf334Ooj@"
    "foccuz-db-production.cskc3p3s4ugd.sa-east-1.rds.amazonaws.com:5432/tenant_data"
)


@dataclass
class FoccuzConnectionConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    tenant_id: str

    @classmethod
    def from_config(cls, config: dict) -> 'FoccuzConnectionConfig':
        connection_string = config.get('connection_string') or os.environ.get(
            'FOCCUZ_DATABASE_URL', DEFAULT_CONNECTION_STRING
        )

        parts = cls._parse_connection_string(connection_string)

        return cls(
            host=config.get('host') or parts.get('host', ''),
            port=config.get('port') or parts.get('port', 5432),
            database=config.get('database') or parts.get('database', 'tenant_data'),
            user=config.get('user') or parts.get('user', ''),
            password=config.get('password') or parts.get('password', ''),
            tenant_id=config.get('tenant_id', ''),
        )

    @staticmethod
    def _parse_connection_string(conn_str: str) -> dict:
        try:
            if conn_str.startswith('postgresql://'):
                conn_str = conn_str[13:]

            user_pass, host_db = conn_str.split('@')
            user, password = user_pass.split(':')
            host_port, database = host_db.split('/')
            host, port = host_port.split(':')

            return {
                'host': host,
                'port': int(port),
                'database': database,
                'user': user,
                'password': password,
            }
        except (ValueError, IndexError):
            return {}
