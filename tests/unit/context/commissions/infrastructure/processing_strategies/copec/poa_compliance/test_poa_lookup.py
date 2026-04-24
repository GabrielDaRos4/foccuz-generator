from datetime import datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.poa_compliance.poa_lookup import (
    INTERNAL_TO_POA_MARGEN,
    INTERNAL_TO_POA_VOLUMEN,
    PoaLookup,
)


class TestPoaLookup:

    @pytest.fixture
    def poa_data(self):
        return pd.DataFrame({
            "Rut": ["12.031.219-7", "12.031.219-7", "12.031.219-7", "18.991.169-6"],
            "Producto": ["TCT (M3)", "TAE (M3)", "CE (M3)", "TCT (M3)"],
            datetime(2025, 10, 1): [5046.0, 3043.0, 217.0, 1000.0],
            datetime(2025, 11, 1): [5175.0, 3061.0, 234.0, 1100.0],
        })

    @pytest.fixture
    def period(self):
        return datetime(2025, 10, 1)


class TestLookupVolumen(TestPoaLookup):

    def test_should_find_poa_value_for_matching_rep_and_product(self, poa_data, period):
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12031219-7", "TCT")

        assert result == 5046.0

    def test_should_return_none_when_rep_not_found(self, poa_data, period):
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("99999999-9", "TCT")

        assert result is None

    def test_should_return_none_when_product_not_mapped(self, poa_data, period):
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12031219-7", "UnknownProduct")

        assert result is None

    def test_should_sanitize_rut_by_removing_dots(self, poa_data, period):
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12.031.219-7", "TCT")

        assert result == 5046.0

    def test_should_find_tae_product(self, poa_data, period):
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12031219-7", "TAE")

        assert result == 3043.0

    def test_should_find_ce_product(self, poa_data, period):
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12031219-7", "CE")

        assert result == 217.0

    def test_should_find_bluemax_tct_using_bm_poa(self, period):
        poa_data = pd.DataFrame({
            "Rut": ["12.031.219-7"],
            "Producto": ["BM (M3)"],
            datetime(2025, 10, 1): [500.0],
        })
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12031219-7", "Bluemax TCT")

        assert result == 500.0

    def test_should_find_bluemax_appce_using_bm_poa(self, period):
        poa_data = pd.DataFrame({
            "Rut": ["12.031.219-7"],
            "Producto": ["BM (M3)"],
            datetime(2025, 10, 1): [500.0],
        })
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12031219-7", "Bluemax AppCE")

        assert result == 500.0

    def test_should_normalize_app_ce_with_space_to_appce(self, period):
        poa_data = pd.DataFrame({
            "Rut": ["12.031.219-7"],
            "Producto": ["App CE (M3)"],
            datetime(2025, 10, 1): [300.0],
        })
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12031219-7", "AppCE")

        assert result == 300.0

    def test_should_normalize_ce_plus_app_ce_with_space(self, period):
        poa_data = pd.DataFrame({
            "Rut": ["12.031.219-7"],
            "Producto": ["CE + App CE (M3)"],
            datetime(2025, 10, 1): [600.0],
        })
        lookup = PoaLookup(poa_data, "volumen", period)

        result = lookup.lookup("12031219-7", "CE + AppCE")

        assert result == 600.0


class TestLookupMargen(TestPoaLookup):

    @pytest.fixture
    def poa_data_margen(self):
        return pd.DataFrame({
            "Rut": ["12.031.219-7", "12.031.219-7"],
            "Producto": ["TCT ($/L)", "TAE ($/L)"],
            datetime(2025, 10, 1): [50.8, 53.3],
        })

    def test_should_find_poa_value_for_margin_product(self, poa_data_margen, period):
        lookup = PoaLookup(poa_data_margen, "margen", period)

        result = lookup.lookup("12031219-7", "TCT")

        assert result == 50.8


class TestPeriodColumn(TestPoaLookup):

    def test_should_find_correct_period_column(self, poa_data, period):
        lookup = PoaLookup(poa_data, "volumen", period)

        assert lookup._period_column == datetime(2025, 10, 1)

    def test_should_return_none_when_period_not_found(self, poa_data):
        period = datetime(2025, 12, 1)
        lookup = PoaLookup(poa_data, "volumen", period)

        assert lookup._period_column is None

    def test_should_not_be_available_when_period_not_found(self, poa_data):
        period = datetime(2025, 12, 1)
        lookup = PoaLookup(poa_data, "volumen", period)

        assert lookup.is_available() is False


class TestIsAvailable(TestPoaLookup):

    def test_should_be_available_when_data_and_period_exist(self, poa_data, period):
        lookup = PoaLookup(poa_data, "volumen", period)

        assert lookup.is_available() is True

    def test_should_not_be_available_when_dataframe_empty(self, period):
        empty_df = pd.DataFrame()
        lookup = PoaLookup(empty_df, "volumen", period)

        assert lookup.is_available() is False

    def test_should_not_be_available_when_dataframe_none(self, period):
        lookup = PoaLookup(None, "volumen", period)

        assert lookup.is_available() is False


class TestProductMappings:

    def test_should_map_tct_to_poa_for_volumen(self):
        assert INTERNAL_TO_POA_VOLUMEN["TCT"] == "TCT (M3)"

    def test_should_map_tae_to_poa_for_volumen(self):
        assert INTERNAL_TO_POA_VOLUMEN["TAE"] == "TAE (M3)"

    def test_should_map_bluemax_total_to_bm_for_volumen(self):
        assert INTERNAL_TO_POA_VOLUMEN["Bluemax Total"] == "BM (M3)"

    def test_should_map_bluemax_tct_to_bm_for_volumen(self):
        assert INTERNAL_TO_POA_VOLUMEN["Bluemax TCT"] == "BM (M3)"

    def test_should_map_bluemax_appce_to_bm_for_volumen(self):
        assert INTERNAL_TO_POA_VOLUMEN["Bluemax AppCE"] == "BM (M3)"

    def test_should_map_lubricantes_to_poa_for_volumen(self):
        assert INTERNAL_TO_POA_VOLUMEN["Lubricantes"] == "LUB (L)"

    def test_should_map_tctp_to_poa_for_volumen(self):
        assert INTERNAL_TO_POA_VOLUMEN["TCTP"] == "TCTP (N° Patentes)"

    def test_should_map_ce_plus_appce_to_poa_for_volumen(self):
        assert INTERNAL_TO_POA_VOLUMEN["CE + AppCE"] == "CE + AppCE (M3)"

    def test_should_map_tct_to_poa_for_margen(self):
        assert INTERNAL_TO_POA_MARGEN["TCT"] == "TCT ($/L)"

    def test_should_map_tae_to_poa_for_margen(self):
        assert INTERNAL_TO_POA_MARGEN["TAE"] == "TAE ($/L)"

    def test_should_map_bluemax_tct_to_bm_for_margen(self):
        assert INTERNAL_TO_POA_MARGEN["Bluemax TCT"] == "BM ($/L)"

    def test_should_map_bluemax_appce_to_bm_for_margen(self):
        assert INTERNAL_TO_POA_MARGEN["Bluemax AppCE"] == "BM ($/L)"
