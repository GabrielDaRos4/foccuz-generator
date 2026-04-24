import pytest

from src.context.commissions.domain.value_objects import (
    DataMergeConfig,
    DataSourceCollection,
    DataSourceConfig,
)


class TestDataSourceConfig:

    def test_create_valid_source(self):
        source = DataSourceConfig(
            source_id="ventas_s3",
            source_type="s3",
            config={"bucket": "test", "key": "data.csv"}
        )
        assert source.source_id == "ventas_s3"
        assert source.source_type == "s3"

    def test_requires_source_id(self):
        with pytest.raises(ValueError, match="source_id cannot be empty"):
            DataSourceConfig(source_id="", source_type="s3", config={})

    def test_requires_source_type(self):
        with pytest.raises(ValueError, match="source_type cannot be empty"):
            DataSourceConfig(source_id="test", source_type="", config={})

    def test_requires_config_dict(self):
        with pytest.raises(ValueError, match="config must be a dictionary"):
            DataSourceConfig(source_id="test", source_type="s3", config="invalid")


class TestDataSourceCollection:

    def test_single_source_collection(self):
        source = DataSourceConfig("s3_ventas", "s3", {"bucket": "test"})
        collection = DataSourceCollection(sources=[source])

        assert collection.is_single_source()
        assert len(collection.sources) == 1
        assert collection.get_primary_source() == source

    def test_multi_source_requires_merge_strategy(self):
        source1 = DataSourceConfig("ventas", "s3", {})
        source2 = DataSourceConfig("empleados", "buk", {})

        with pytest.raises(ValueError, match="merge_strategy is required"):
            DataSourceCollection(sources=[source1, source2])

    def test_multi_source_with_valid_merge_strategy(self):
        source1 = DataSourceConfig("ventas", "s3", {})
        source2 = DataSourceConfig("empleados", "buk", {})

        merge_strategy = DataMergeConfig(
            merge_type="join",
            primary_source_id="ventas",
            merge_config={"joins": []}
        )

        collection = DataSourceCollection(
            sources=[source1, source2],
            merge_strategy=merge_strategy
        )

        assert not collection.is_single_source()
        assert len(collection.sources) == 2
        assert collection.get_primary_source() == source1

    def test_duplicate_source_ids_raise_error(self):
        source1 = DataSourceConfig("ventas", "s3", {})
        source2 = DataSourceConfig("ventas", "buk", {})

        merge_strategy = DataMergeConfig(
            merge_type="join",
            primary_source_id="ventas",
            merge_config={}
        )

        with pytest.raises(ValueError, match="Duplicate source_ids"):
            DataSourceCollection(sources=[source1, source2], merge_strategy=merge_strategy)

    def test_primary_source_must_exist(self):
        source1 = DataSourceConfig("ventas", "s3", {})
        source2 = DataSourceConfig("empleados", "buk", {})

        merge_strategy = DataMergeConfig(
            merge_type="join",
            primary_source_id="nonexistent",
            merge_config={}
        )

        with pytest.raises(ValueError, match="not found in sources"):
            DataSourceCollection(sources=[source1, source2], merge_strategy=merge_strategy)

    def test_get_source(self):
        source1 = DataSourceConfig("ventas", "s3", {})
        source2 = DataSourceConfig("empleados", "buk", {})

        merge_strategy = DataMergeConfig(
            merge_type="join",
            primary_source_id="ventas",
            merge_config={}
        )

        collection = DataSourceCollection(
            sources=[source1, source2],
            merge_strategy=merge_strategy
        )

        assert collection.get_source("ventas") == source1
        assert collection.get_source("empleados") == source2

        with pytest.raises(ValueError, match="not found"):
            collection.get_source("nonexistent")

    def test_has_source(self):
        source = DataSourceConfig("ventas", "s3", {})
        collection = DataSourceCollection(sources=[source])

        assert collection.has_source("ventas")
        assert not collection.has_source("nonexistent")

    def test_get_source_ids(self):
        source1 = DataSourceConfig("ventas", "s3", {})
        source2 = DataSourceConfig("empleados", "buk", {})

        merge_strategy = DataMergeConfig(
            merge_type="join",
            primary_source_id="ventas",
            merge_config={}
        )

        collection = DataSourceCollection(
            sources=[source1, source2],
            merge_strategy=merge_strategy
        )

        assert set(collection.get_source_ids()) == {"ventas", "empleados"}

    def test_requires_at_least_one_source(self):
        with pytest.raises(ValueError, match="At least one data source"):
            DataSourceCollection(sources=[])

    def test_get_sources_by_type(self):
        s3 = DataSourceConfig("ventas", "s3", {})
        buk = DataSourceConfig("empleados", "buk", {})
        merge = DataMergeConfig(merge_type="join", primary_source_id="ventas", merge_config={})
        collection = DataSourceCollection(sources=[s3, buk], merge_strategy=merge)

        s3_sources = collection.get_sources_by_type("s3")
        assert len(s3_sources) == 1
        assert s3_sources[0].source_id == "ventas"

        csv_sources = collection.get_sources_by_type("csv")
        assert len(csv_sources) == 0

