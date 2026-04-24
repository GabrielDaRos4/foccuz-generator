from dataclasses import dataclass

from src.context.shared.domain import ValueObject

from .data_merge_config import DataMergeConfig
from .data_source_config import DataSourceConfig


@dataclass(frozen=True)
class DataSourceCollection(ValueObject):
    sources: tuple
    merge_strategy: DataMergeConfig | None = None

    def __init__(self, sources: list[DataSourceConfig], merge_strategy: DataMergeConfig | None = None):
        object.__setattr__(self, 'sources', tuple(sources) if sources else ())
        object.__setattr__(self, 'merge_strategy', merge_strategy)
        self.__post_init__()

    def __post_init__(self):
        if not self.sources:
            raise ValueError("At least one data source is required")

        source_ids = [s.source_id for s in self.sources]
        if len(source_ids) != len(set(source_ids)):
            duplicates = [sid for sid in source_ids if source_ids.count(sid) > 1]
            raise ValueError(f"Duplicate source_ids found: {set(duplicates)}")

        if len(self.sources) > 1 and not self.merge_strategy:
            raise ValueError("merge_strategy is required when using multiple data sources")

        if self.merge_strategy:
            if not self.has_source(self.merge_strategy.primary_source_id):
                raise ValueError(
                    f"primary_source_id '{self.merge_strategy.primary_source_id}' "
                    f"not found in sources"
                )

    def is_single_source(self) -> bool:
        return len(self.sources) == 1 and self.merge_strategy is None

    def has_source(self, source_id: str) -> bool:
        return any(s.source_id == source_id for s in self.sources)

    def get_source(self, source_id: str) -> DataSourceConfig:
        for source in self.sources:
            if source.source_id == source_id:
                return source
        raise ValueError(f"Source '{source_id}' not found in collection")

    def get_primary_source(self) -> DataSourceConfig:
        if self.merge_strategy:
            return self.get_source(self.merge_strategy.primary_source_id)
        return self.sources[0]

    def get_source_ids(self) -> list[str]:
        return [s.source_id for s in self.sources]

    def get_sources_by_type(self, source_type: str) -> list[DataSourceConfig]:
        return [s for s in self.sources if s.source_type == source_type]
