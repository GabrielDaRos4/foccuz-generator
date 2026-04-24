from pathlib import Path

from .plan_config import PlanConfig, YamlConfig


class TenantConfig:
    def __init__(self, config_path: Path, config: YamlConfig):
        self.config_path = config_path
        metadata = config.get('metadata', {})

        self.client_id = metadata.get('client_id', '')
        self.client_name = metadata.get('client_name', '')
        self.gsheet_output = metadata.get('gsheet_output', '')
        self.active = metadata.get('active', False)

        self.plans: dict[str, PlanConfig] = {}
        for plan_id, plan_config in config.get('plans', {}).items():
            self.plans[plan_id] = PlanConfig(
                self.client_id,
                plan_id,
                plan_config,
                default_sheet_id=self.gsheet_output
            )
