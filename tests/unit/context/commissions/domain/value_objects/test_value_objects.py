from datetime import datetime

import pytest

from src.context.commissions.domain.value_objects import (
    DataMergeConfig,
    OutputConfig,
    StrategyConfig,
    ValidityPeriod,
)


class TestOutputConfig:

    def test_create_valid_output_config(self):
        config = OutputConfig(
            sheet_id="sheet123",
            tab_name="Tab1"
        )
        assert config.sheet_id == "sheet123"
        assert config.tab_name == "Tab1"
        assert config.clear_before_write is True

    def test_create_with_clear_before_write_false(self):
        config = OutputConfig(
            sheet_id="sheet123",
            tab_name="Tab1",
            clear_before_write=False
        )
        assert config.clear_before_write is False

    def test_requires_sheet_id(self):
        with pytest.raises(ValueError, match="Sheet ID cannot be empty"):
            OutputConfig(sheet_id="", tab_name="Tab1")

    def test_requires_tab_name(self):
        with pytest.raises(ValueError, match="Tab name cannot be empty"):
            OutputConfig(sheet_id="sheet123", tab_name="")


class TestStrategyConfig:

    def test_create_valid_strategy_config(self):
        config = StrategyConfig(
            module="src.context.commissions.infrastructure.strategies",
            class_name="TieredCommissionStrategy",
            params={"tiers": [{"min": 0, "max": 1000, "rate": 0.05}]}
        )
        assert config.module == "src.context.commissions.infrastructure.strategies"
        assert config.class_name == "TieredCommissionStrategy"
        assert "tiers" in config.params

    def test_create_with_empty_params(self):
        config = StrategyConfig(
            module="test.module",
            class_name="TestStrategy",
            params={}
        )
        assert config.params == {}

    def test_requires_module(self):
        with pytest.raises(ValueError, match="Module cannot be empty"):
            StrategyConfig(module="", class_name="Test", params={})

    def test_requires_class_name(self):
        with pytest.raises(ValueError, match="Class name cannot be empty"):
            StrategyConfig(module="test.module", class_name="", params={})


class TestValidityPeriod:

    def test_create_default_validity_period(self):
        period = ValidityPeriod()
        assert period.valid_from is None
        assert period.valid_until is None

    def test_create_with_valid_from(self):
        valid_from = datetime(2025, 1, 1)
        period = ValidityPeriod(valid_from=valid_from)
        assert period.valid_from == valid_from
        assert period.valid_until is None

    def test_create_with_valid_until(self):
        valid_until = datetime(2025, 12, 31)
        period = ValidityPeriod(valid_until=valid_until)
        assert period.valid_from is None
        assert period.valid_until == valid_until

    def test_create_with_both_dates(self):
        valid_from = datetime(2025, 1, 1)
        valid_until = datetime(2025, 12, 31)
        period = ValidityPeriod(valid_from=valid_from, valid_until=valid_until)
        assert period.valid_from == valid_from
        assert period.valid_until == valid_until

    def test_is_valid_at_with_no_constraints(self):
        period = ValidityPeriod()
        assert period.is_valid_at(datetime(2025, 6, 15)) is True

    def test_is_valid_at_before_valid_from(self):
        period = ValidityPeriod(valid_from=datetime(2025, 6, 1))
        assert period.is_valid_at(datetime(2025, 5, 15)) is False

    def test_is_valid_at_after_valid_from(self):
        period = ValidityPeriod(valid_from=datetime(2025, 6, 1))
        assert period.is_valid_at(datetime(2025, 6, 15)) is True

    def test_is_valid_at_before_valid_until(self):
        period = ValidityPeriod(valid_until=datetime(2025, 12, 31))
        assert period.is_valid_at(datetime(2025, 6, 15)) is True

    def test_is_valid_at_after_valid_until(self):
        period = ValidityPeriod(valid_until=datetime(2025, 12, 31))
        assert period.is_valid_at(datetime(2026, 1, 15)) is False

    def test_is_valid_at_within_range(self):
        period = ValidityPeriod(
            valid_from=datetime(2025, 1, 1),
            valid_until=datetime(2025, 12, 31)
        )
        assert period.is_valid_at(datetime(2025, 6, 15)) is True

    def test_is_valid_at_outside_range(self):
        period = ValidityPeriod(
            valid_from=datetime(2025, 1, 1),
            valid_until=datetime(2025, 12, 31)
        )
        assert period.is_valid_at(datetime(2024, 6, 15)) is False
        assert period.is_valid_at(datetime(2026, 6, 15)) is False


class TestDataMergeConfig:

    def test_create_valid_merge_config(self):
        config = DataMergeConfig(
            merge_type="join",
            primary_source_id="ventas",
            merge_config={"left_on": "id", "right_on": "sale_id"}
        )
        assert config.merge_type == "join"
        assert config.primary_source_id == "ventas"
        assert config.merge_config["left_on"] == "id"

    def test_requires_merge_type(self):
        with pytest.raises(ValueError, match="merge_type cannot be empty"):
            DataMergeConfig(merge_type="", primary_source_id="ventas", merge_config={})

    def test_requires_primary_source_id(self):
        with pytest.raises(ValueError, match="primary_source_id cannot be empty"):
            DataMergeConfig(merge_type="join", primary_source_id="", merge_config={})

    def test_requires_merge_config_dict(self):
        with pytest.raises(ValueError, match="merge_config must be a dictionary"):
            DataMergeConfig(merge_type="join", primary_source_id="ventas", merge_config="invalid")
