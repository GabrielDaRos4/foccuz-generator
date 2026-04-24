import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.quarterly_team import (
    quarterly_team_merge as _qtm,
)

SOURCE_PATTERNS = _qtm.SOURCE_PATTERNS
_collect_source_data = _qtm._collect_source_data
copec_quarterly_team_merge = _qtm.copec_quarterly_team_merge


class TestCopecQuarterlyTeamMerge:

    @pytest.fixture
    def sample_ejecutivos(self):
        return pd.DataFrame({
            "RUT": ["12345678-9"],
            "Nombre": ["John Doe"],
        })

    @pytest.fixture
    def sample_poa(self):
        return pd.DataFrame({
            "RUT": ["12345678-9"],
            "Meta": [1000],
        })

    @pytest.fixture
    def sample_tct_tae(self):
        return pd.DataFrame({
            "RUT": ["12345678-9"],
            "Volumen": [500],
        })


class TestMerge(TestCopecQuarterlyTeamMerge):

    def test_should_return_dataframe_with_sources_in_attrs(
        self, sample_ejecutivos, sample_poa
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "poa_resumen": sample_poa,
        }

        result = copec_quarterly_team_merge(dataframes)

        assert "sources" in result.attrs

    def test_should_include_ejecutivos_in_sources(
        self, sample_ejecutivos, sample_poa
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "poa_resumen": sample_poa,
        }

        result = copec_quarterly_team_merge(dataframes)

        assert "ejecutivos" in result.attrs["sources"]

    def test_should_include_poa_in_sources(
        self, sample_ejecutivos, sample_poa
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "poa_resumen": sample_poa,
        }

        result = copec_quarterly_team_merge(dataframes)

        assert "POA_RESUMEN" in result.attrs["sources"]

    def test_should_raise_error_when_no_ejecutivos(self, sample_poa):
        dataframes = {
            "poa_resumen": sample_poa,
        }

        with pytest.raises(ValueError, match="Ejecutivos data required"):
            copec_quarterly_team_merge(dataframes)

    def test_should_raise_error_when_no_poa(self, sample_ejecutivos):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
        }

        with pytest.raises(ValueError, match="POA data required"):
            copec_quarterly_team_merge(dataframes)

    def test_should_include_target_period_from_config(
        self, sample_ejecutivos, sample_poa
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "poa_resumen": sample_poa,
        }
        config = {"target_period": "2025-01-01"}

        result = copec_quarterly_team_merge(dataframes, config)

        assert result.attrs.get("target_period") == "2025-01-01"

    def test_should_load_multiple_sources(
        self, sample_ejecutivos, sample_poa, sample_tct_tae
    ):
        dataframes = {
            "ejecutivos": sample_ejecutivos,
            "poa_resumen": sample_poa,
            "tct_tae": sample_tct_tae,
        }

        result = copec_quarterly_team_merge(dataframes)

        assert "TCT_TAE" in result.attrs["sources"]


class TestCollectSourceData:

    def test_should_find_source_by_pattern(self):
        dataframes = {
            "my_ejecutivos_data": pd.DataFrame({"col": [1]}),
        }

        result = _collect_source_data(
            dataframes,
            ["ejecutivos"],
            "ejecutivos"
        )

        assert result is not None
        assert len(result) == 1

    def test_should_return_none_when_no_match(self):
        dataframes = {
            "other_data": pd.DataFrame({"col": [1]}),
        }

        result = _collect_source_data(
            dataframes,
            ["ejecutivos"],
            "ejecutivos"
        )

        assert result is None

    def test_should_skip_empty_dataframes(self):
        dataframes = {
            "ejecutivos": pd.DataFrame(),
            "ejecutivos_backup": pd.DataFrame({"col": [1]}),
        }

        result = _collect_source_data(
            dataframes,
            ["ejecutivos"],
            "ejecutivos"
        )

        assert result is not None
        assert len(result) == 1

    def test_should_match_case_insensitively(self):
        dataframes = {
            "EJECUTIVOS": pd.DataFrame({"col": [1]}),
        }

        result = _collect_source_data(
            dataframes,
            ["ejecutivos"],
            "ejecutivos"
        )

        assert result is not None


class TestSourcePatterns:

    def test_should_have_ejecutivos_pattern(self):
        assert "ejecutivos" in SOURCE_PATTERNS

    def test_should_have_poa_resumen_pattern(self):
        assert "POA_RESUMEN" in SOURCE_PATTERNS

    def test_should_have_tct_tae_pattern(self):
        assert "TCT_TAE" in SOURCE_PATTERNS

    def test_should_have_lubricantes_pattern(self):
        assert "LUBRICANTES" in SOURCE_PATTERNS

    def test_ejecutivos_patterns_include_info_ejecutivos(self):
        assert "info-ejecutivos" in SOURCE_PATTERNS["ejecutivos"]
