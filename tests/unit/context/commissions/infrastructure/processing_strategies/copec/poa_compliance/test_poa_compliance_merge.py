import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.poa_compliance import (
    copec_poa_compliance_merge,
)


class TestCopecPoaComplianceMerge:

    @pytest.fixture
    def tct_tae_data(self):
        return pd.DataFrame({
            "anio": [2025, 2025],
            "mes": [10, 10],
            "producto": ["TCT", "TAE"],
            "volumen": [1000, 2000],
            "contribucion": [50000, 80000],
        })

    @pytest.fixture
    def cupon_electronico_data(self):
        return pd.DataFrame({
            "anio": [2025],
            "mes": [10],
            "producto": ["Cupon Electronico"],
            "volumen": [500],
            "contribucion": [25000],
        })

    @pytest.fixture
    def app_copec_data(self):
        return pd.DataFrame({
            "anio": [2025, 2025],
            "mes": [10, 10],
            "producto": ["App Copec Empresa Combustible", "App Copec Empresa Bluemax"],
            "volumen": [300, 150],
        })

    @pytest.fixture
    def bluemax_data(self):
        return pd.DataFrame({
            "anio": [2025, 2025],
            "mes": [10, 10],
            "producto": ["Bluemax Indirecto TCT", "Bluemax Directo TCT"],
            "volumen_lts": [400, 600],
            "contribucion": [20000, 30000],
        })

    @pytest.fixture
    def lubricantes_data(self):
        return pd.DataFrame({
            "Rut": ["12345678-9"],
            "Vendedor": ["Juan Perez"],
            "volumen": [750],
        })

    @pytest.fixture
    def tct_premium_data(self):
        return pd.DataFrame({
            "anio": [2025],
            "mes": [10],
            "producto": ["TCT Premium"],
            "volumen_tct_premium": [1200],
        })


class TestMergeWithValidData(TestCopecPoaComplianceMerge):

    def test_should_return_dataframe_with_sources_attr_when_data_provided(
        self, tct_tae_data, cupon_electronico_data
    ):
        dataframes = {
            "tct_tae_actual": tct_tae_data,
            "cupon_electronico_actual": cupon_electronico_data,
        }

        result = copec_poa_compliance_merge(dataframes)

        assert "sources" in result.attrs
        assert len(result.attrs["sources"]) >= 2

    def test_should_load_tct_tae_source_when_matching_key_exists(self, tct_tae_data):
        dataframes = {"tct_tae_actual": tct_tae_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "TCT_TAE" in result.attrs["sources"]
        assert len(result.attrs["sources"]["TCT_TAE"]) == 2

    def test_should_load_cupon_electronico_source_when_matching_key_exists(
        self, cupon_electronico_data
    ):
        dataframes = {"cupon_electronico_actual": cupon_electronico_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "CUPON_ELECTRONICO" in result.attrs["sources"]

    def test_should_load_app_copec_source_when_matching_key_exists(self, app_copec_data):
        dataframes = {"app_copec_actual": app_copec_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "APP_COPEC" in result.attrs["sources"]

    def test_should_load_bluemax_source_when_matching_key_exists(self, bluemax_data):
        dataframes = {"bluemax_actual": bluemax_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "BLUEMAX" in result.attrs["sources"]

    def test_should_load_lubricantes_source_when_matching_key_exists(self, lubricantes_data):
        dataframes = {"lubricantes": lubricantes_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "LUBRICANTES" in result.attrs["sources"]

    def test_should_load_lubricantes_with_detalle_key(self, lubricantes_data):
        dataframes = {"detalle_comision_mes": lubricantes_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "LUBRICANTES" in result.attrs["sources"]

    def test_should_load_tct_premium_source_when_matching_key_exists(self, tct_premium_data):
        dataframes = {"tct_premium_actual": tct_premium_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "TCT_PREMIUM" in result.attrs["sources"]

    def test_should_store_target_period_in_attrs_when_config_provided(self, tct_tae_data):
        dataframes = {"tct_tae_actual": tct_tae_data}
        config = {"target_period": "2025-10-01"}

        result = copec_poa_compliance_merge(dataframes, config)

        assert result.attrs["target_period"] == "2025-10-01"


class TestMergeWithTargetPeriodFilter(TestCopecPoaComplianceMerge):

    def test_should_filter_data_by_target_period_when_multiple_periods_exist(self):
        october_data = pd.DataFrame({
            "anio": [2025],
            "mes": [10],
            "producto": ["TCT"],
            "volumen": [1000],
        })
        november_data = pd.DataFrame({
            "anio": [2025],
            "mes": [11],
            "producto": ["TCT"],
            "volumen": [2000],
        })
        dataframes = {
            "tct_tae_october": october_data,
            "tct_tae_november": november_data,
        }
        config = {"target_period": "2025-10-01"}

        result = copec_poa_compliance_merge(dataframes, config)

        source_df = result.attrs["sources"]["TCT_TAE"]
        assert source_df["volumen"].iloc[0] == 1000

    def test_should_use_first_available_when_no_period_match(self):
        november_data = pd.DataFrame({
            "anio": [2025],
            "mes": [11],
            "producto": ["TCT"],
            "volumen": [2000],
        })
        dataframes = {"tct_tae_actual": november_data}
        config = {"target_period": "2025-10-01"}

        result = copec_poa_compliance_merge(dataframes, config)

        assert "TCT_TAE" in result.attrs["sources"]
        assert result.attrs["sources"]["TCT_TAE"]["volumen"].iloc[0] == 2000

    def test_should_use_data_without_period_columns_directly(self, lubricantes_data):
        dataframes = {"lubricantes": lubricantes_data}
        config = {"target_period": "2025-10-01"}

        result = copec_poa_compliance_merge(dataframes, config)

        assert "LUBRICANTES" in result.attrs["sources"]


class TestMergeWithInvalidData(TestCopecPoaComplianceMerge):

    def test_should_raise_error_when_no_sources_found(self):
        dataframes = {"unrelated_key": pd.DataFrame({"col": [1]})}

        with pytest.raises(ValueError, match="No source data found"):
            copec_poa_compliance_merge(dataframes)

    def test_should_raise_error_when_dataframes_empty(self):
        with pytest.raises(ValueError, match="No source data found"):
            copec_poa_compliance_merge({})

    def test_should_skip_empty_dataframes_when_loading_sources(self, tct_tae_data):
        dataframes = {
            "tct_tae_actual": tct_tae_data,
            "cupon_electronico_actual": pd.DataFrame(),
        }

        result = copec_poa_compliance_merge(dataframes)

        assert "TCT_TAE" in result.attrs["sources"]
        assert "CUPON_ELECTRONICO" not in result.attrs["sources"]


class TestCaseInsensitiveMatching(TestCopecPoaComplianceMerge):

    def test_should_match_uppercase_keys(self, tct_tae_data):
        dataframes = {"TCT_TAE_ACTUAL": tct_tae_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "TCT_TAE" in result.attrs["sources"]

    def test_should_match_mixed_case_keys(self, bluemax_data):
        dataframes = {"Bluemax_Actual": bluemax_data}

        result = copec_poa_compliance_merge(dataframes)

        assert "BLUEMAX" in result.attrs["sources"]
