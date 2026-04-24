from datetime import datetime

from src.context.commissions.domain.value_objects import (
    DataMergeConfig,
    DataSourceCollection,
    DataSourceConfig,
    OutputConfig,
    StrategyConfig,
    ValidityPeriod,
)


class DataSourceConfigMother:

    @staticmethod
    def csv(source_id: str = "default", path: str = "data.csv") -> DataSourceConfig:
        return DataSourceConfig(
            source_id=source_id,
            source_type="csv",
            config={"path": path, "separator": ",", "encoding": "utf-8"}
        )

    @staticmethod
    def s3(source_id: str = "s3_source", bucket: str = "test-bucket", key: str = "data.csv") -> DataSourceConfig:
        return DataSourceConfig(
            source_id=source_id,
            source_type="s3",
            config={"bucket": bucket, "key": key}
        )

    @staticmethod
    def json(source_id: str = "json_source", path: str = "data.json") -> DataSourceConfig:
        return DataSourceConfig(
            source_id=source_id,
            source_type="json",
            config={"path": path}
        )

    @staticmethod
    def buk(source_id: str = "empleados") -> DataSourceConfig:
        return DataSourceConfig(
            source_id=source_id,
            source_type="buk",
            config={"endpoint": "employees"}
        )


class DataMergeConfigMother:

    @staticmethod
    def join(primary_source_id: str = "ventas", left_on: str = "id", right_on: str = "id") -> DataMergeConfig:
        return DataMergeConfig(
            merge_type="join",
            primary_source_id=primary_source_id,
            merge_config={"left_on": left_on, "right_on": right_on, "how": "left"}
        )

    @staticmethod
    def concat(primary_source_id: str = "source1") -> DataMergeConfig:
        return DataMergeConfig(
            merge_type="concat",
            primary_source_id=primary_source_id,
            merge_config={"axis": 0}
        )

    @staticmethod
    def custom(primary_source_id: str = "ventas", strategy_name: str = "custom_merge") -> DataMergeConfig:
        return DataMergeConfig(
            merge_type="custom",
            primary_source_id=primary_source_id,
            merge_config={"strategy_name": strategy_name}
        )


class DataSourceCollectionMother:

    @staticmethod
    def single_csv(source_id: str = "default") -> DataSourceCollection:
        source = DataSourceConfigMother.csv(source_id)
        return DataSourceCollection(sources=[source])

    @staticmethod
    def single_s3(source_id: str = "s3_source") -> DataSourceCollection:
        source = DataSourceConfigMother.s3(source_id)
        return DataSourceCollection(sources=[source])

    @staticmethod
    def multi_source_with_join() -> DataSourceCollection:
        ventas = DataSourceConfigMother.s3("ventas")
        empleados = DataSourceConfigMother.buk("empleados")
        merge = DataMergeConfigMother.join("ventas")
        return DataSourceCollection(sources=[ventas, empleados], merge_strategy=merge)


class OutputConfigMother:

    @staticmethod
    def default(sheet_id: str = "sheet123", tab_name: str = "Tab1") -> OutputConfig:
        return OutputConfig(
            sheet_id=sheet_id,
            tab_name=tab_name,
            clear_before_write=True
        )

    @staticmethod
    def append_mode(sheet_id: str = "sheet123", tab_name: str = "Tab1") -> OutputConfig:
        return OutputConfig(
            sheet_id=sheet_id,
            tab_name=tab_name,
            clear_before_write=False
        )


class StrategyConfigMother:

    @staticmethod
    def tiered_commission() -> StrategyConfig:
        return StrategyConfig(
            module="src.context.commissions.infrastructure.processing_strategies.standard.tiered_commission",
            class_name="TieredCommissionStrategy",
            params={
                "tiers": [
                    {"min": 0, "max": 10000, "rate": 0.05},
                    {"min": 10000, "max": 50000, "rate": 0.10},
                    {"min": 50000, "max": None, "rate": 0.15}
                ]
            }
        )

    @staticmethod
    def copec_new_client(product_type: str = "TCT") -> StrategyConfig:
        return StrategyConfig(
            module="src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client_commission_strategy",
            class_name="CopecNewClientCommissionStrategy",
            params={
                "product_type": product_type,
                "discount_percentage": 0.08,
                "max_factor": 6.0,
                "bono_nuevo": 10000,
                "factor_minimo": 0.5
            }
        )

    @staticmethod
    def custom(module: str, class_name: str, params: dict = None) -> StrategyConfig:
        return StrategyConfig(
            module=module,
            class_name=class_name,
            params=params or {}
        )


class ValidityPeriodMother:

    @staticmethod
    def always_valid() -> ValidityPeriod:
        return ValidityPeriod()

    @staticmethod
    def year_2025() -> ValidityPeriod:
        return ValidityPeriod(
            valid_from=datetime(2025, 1, 1),
            valid_until=datetime(2025, 12, 31)
        )

    @staticmethod
    def expired() -> ValidityPeriod:
        return ValidityPeriod(
            valid_from=datetime(2020, 1, 1),
            valid_until=datetime(2020, 12, 31)
        )

    @staticmethod
    def future() -> ValidityPeriod:
        return ValidityPeriod(
            valid_from=datetime(2030, 1, 1),
            valid_until=datetime(2030, 12, 31)
        )

    @staticmethod
    def from_date(valid_from: datetime) -> ValidityPeriod:
        return ValidityPeriod(valid_from=valid_from)

    @staticmethod
    def until_date(valid_until: datetime) -> ValidityPeriod:
        return ValidityPeriod(valid_until=valid_until)
