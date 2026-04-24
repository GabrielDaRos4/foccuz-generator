import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.summary.summary_merge import (
    PLAN_TABS,
    copec_summary_merge,
)


class TestCopecSummaryMerge:

    @pytest.fixture
    def sample_ejecutivos(self):
        return pd.DataFrame({
            "RUT": ["12345678-9"],
            "Nombre": ["John Doe"],
        })

    @pytest.fixture
    def sample_plan_data(self):
        return pd.DataFrame({
            "Rep ID": ["12345678-9"],
            "Comision": [10000],
        })


class TestMerge(TestCopecSummaryMerge):

    def test_should_return_dataframe_with_sources_in_attrs(
        self, sample_ejecutivos, sample_plan_data
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "plan_800": sample_plan_data,
        }

        result = copec_summary_merge(dataframes)

        assert "sources" in result.attrs
        assert "PLAN_800" in result.attrs["sources"]

    def test_should_include_ejecutivos_in_sources(
        self, sample_ejecutivos, sample_plan_data
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "plan_800": sample_plan_data,
        }

        result = copec_summary_merge(dataframes)

        assert "ejecutivos" in result.attrs["sources"]

    def test_should_raise_error_when_no_plan_data(self, sample_ejecutivos):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
        }

        with pytest.raises(ValueError, match="No plan data found"):
            copec_summary_merge(dataframes)

    def test_should_include_target_period_from_config(
        self, sample_ejecutivos, sample_plan_data
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "plan_800": sample_plan_data,
        }
        config = {"target_period": "2025-01-01"}

        result = copec_summary_merge(dataframes, config)

        assert result.attrs.get("target_period") == "2025-01-01"

    def test_should_load_multiple_plan_tabs(self, sample_ejecutivos, sample_plan_data):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "plan_800": sample_plan_data,
            "plan_806": sample_plan_data.copy(),
        }

        result = copec_summary_merge(dataframes)

        assert "PLAN_800" in result.attrs["sources"]
        assert "PLAN_806" in result.attrs["sources"]

    def test_should_handle_empty_plan_dataframe(
        self, sample_ejecutivos, sample_plan_data
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "plan_800": sample_plan_data,
            "plan_806": pd.DataFrame(),
        }

        result = copec_summary_merge(dataframes)

        assert "PLAN_800" in result.attrs["sources"]
        assert "PLAN_806" not in result.attrs["sources"]


class TestPlanTabs:

    def test_should_contain_expected_plan_tabs(self):
        expected_tabs = [
            "PLAN_800", "PLAN_806", "PLAN_835", "PLAN_836",
            "PLAN_837", "PLAN_838", "PLAN_839", "PLAN_842",
            "PLAN_786", "PLAN_856"
        ]

        for tab in expected_tabs:
            assert tab in PLAN_TABS

    def test_should_have_correct_number_of_tabs(self):
        assert len(PLAN_TABS) == 10
