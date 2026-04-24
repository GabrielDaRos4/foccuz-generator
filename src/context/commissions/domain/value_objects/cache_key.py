import hashlib
import json
from dataclasses import dataclass

from src.context.shared.domain import ValueObject


@dataclass(frozen=True)
class CacheKey(ValueObject):
    key: str

    @classmethod
    def from_data_source(cls, source_type: str, config: dict) -> 'CacheKey':
        config_str = json.dumps(config, sort_keys=True, default=str)
        key_input = f"{source_type}:{config_str}"
        hash_value = hashlib.sha256(key_input.encode()).hexdigest()[:16]
        return cls(key=f"{source_type}_{hash_value}")

    def __str__(self) -> str:
        return self.key

    def __hash__(self) -> int:
        return hash(self.key)
