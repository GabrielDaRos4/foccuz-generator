from datetime import datetime

from src.context.commissions.domain.value_objects import (
    DataMergeConfig,
    DataSourceCollection,
    DataSourceConfig,
    OutputConfig,
    StrategyConfig,
    ValidityPeriod,
)

YamlValue = str | int | float | bool | None | list['YamlValue'] | dict[str, 'YamlValue']
YamlConfig = dict[str, YamlValue]


def _parse_data_sources(config: YamlConfig) -> DataSourceCollection:

    if 'data_sources' in config:
        sources_list = config['data_sources']

        sources = [
            DataSourceConfig(
                source_id=src.get('id', f'source_{idx}'),
                source_type=src['type'],
                config=src.get('config', {})
            )
            for idx, src in enumerate(sources_list)
        ]

        merge_strategy = None
        if 'data_merge_strategy' in config:
            merge_data = config['data_merge_strategy']
            merge_strategy = DataMergeConfig(
                merge_type=merge_data['type'],
                primary_source_id=merge_data['config'].get('primary_source', sources[0].source_id),
                merge_config=merge_data['config']
            )

        return DataSourceCollection(
            sources=sources,
            merge_strategy=merge_strategy
        )

    elif 'data_source' in config:
        source_data = config['data_source']

        source = DataSourceConfig(
            source_id='default',
            source_type=source_data['type'],
            config=source_data.get('config', {})
        )

        return DataSourceCollection(sources=[source])

    else:
        raise ValueError("Plan must have 'data_source' or 'data_sources'")


class PlanConfig:
    def __init__(
        self,
        tenant_id: str,
        plan_id: str,
        config: YamlConfig,
        default_sheet_id: str = ''
    ):
        self.tenant_id = tenant_id
        self.plan_id = plan_id
        self.name = config.get('name', '')
        self.active = config.get('active', True)

        script_config = config.get('script', {})
        self.strategy_config = StrategyConfig(
            module=script_config.get('module', ''),
            class_name=script_config.get('class', ''),
            params=script_config.get('params', {})
        )

        self.data_source_config = _parse_data_sources(config)

        output = config.get('output', {})
        self.output_config = OutputConfig(
            sheet_id=output.get('sheet_id') or default_sheet_id,
            tab_name=output.get('tab_name', ''),
            clear_before_write=output.get('clear_before_write', True)
        )

        valid_from = config.get('valid_from')
        valid_until = config.get('valid_until')
        self.validity_period = ValidityPeriod(
            valid_from=datetime.fromisoformat(valid_from) if valid_from else None,
            valid_until=datetime.fromisoformat(valid_until) if valid_until else None
        )

        self.depends_on = config.get('depends_on', [])

    @property
    def full_id(self) -> str:
        return f"{self.tenant_id}.{self.plan_id}"
