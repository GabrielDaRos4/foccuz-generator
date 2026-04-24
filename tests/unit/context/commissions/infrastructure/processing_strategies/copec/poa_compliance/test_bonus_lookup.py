import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.poa_compliance.bonus_lookup import (
    METRIC_TYPE_MARGEN,
    METRIC_TYPE_VOLUMEN,
    MIN_COMPLIANCE_THRESHOLD,
    PRODUCT_BONUS_MAPPING_MARGEN,
    PRODUCT_BONUS_MAPPING_VOLUMEN,
    BonusLookup,
)


class TestBonusLookup:

    @pytest.fixture
    def sample_bonus_df(self):
        return pd.DataFrame({
            "RUT": ["12.345.678-9", "98.765.432-1"],
            "Nombre": ["John Doe", "Jane Smith"],
            "TCT_Vol": [10000, 15000],
            "TAE_Vol": [5000, 8000],
            "CE_AppCE_Vol": [3000, 4000],
            "Bluemax_Vol": [2000, 2500],
            "Lub_Vol": [1000, 1500],
            "TCTP": [500, 600],
            "TCT_Mar": [12000, 18000],
            "TAE_Mar": [6000, 9000],
            "Bluemax_Mar": [2500, 3000],
            "Total": [50000, 60000],
        })

    @pytest.fixture
    def volumen_lookup(self, sample_bonus_df):
        return BonusLookup(sample_bonus_df, METRIC_TYPE_VOLUMEN)

    @pytest.fixture
    def margen_lookup(self, sample_bonus_df):
        return BonusLookup(sample_bonus_df, METRIC_TYPE_MARGEN)


class TestLookup(TestBonusLookup):

    def test_should_return_bonus_when_compliance_above_threshold(self, volumen_lookup):
        result = volumen_lookup.lookup("12345678-9", "TCT", 1.0)

        assert result[0] == "10000"
        assert result[1] == 10000

    def test_should_return_zero_when_compliance_below_threshold(self, volumen_lookup):
        result = volumen_lookup.lookup("12345678-9", "TCT", 0.9)

        assert result[0] == "-"
        assert result[1] == 0

    def test_should_return_zero_when_compliance_is_none(self, volumen_lookup):
        result = volumen_lookup.lookup("12345678-9", "TCT", None)

        assert result[0] == "-"
        assert result[1] == 0

    def test_should_return_zero_for_unknown_product(self, volumen_lookup):
        result = volumen_lookup.lookup("12345678-9", "UNKNOWN", 1.0)

        assert result[0] == "0"
        assert result[1] == 0

    def test_should_return_zero_when_rep_not_found(self, volumen_lookup):
        result = volumen_lookup.lookup("99999999-9", "TCT", 1.0)

        assert result[0] == "0"
        assert result[1] == 0

    def test_should_normalize_rut_when_looking_up(self, volumen_lookup):
        result = volumen_lookup.lookup("12.345.678-9", "TCT", 1.0)

        assert result[0] == "10000"
        assert result[1] == 10000

    def test_should_use_margen_mapping_for_margen_type(self, margen_lookup):
        result = margen_lookup.lookup("12345678-9", "TCT", 1.0)

        assert result[0] == "12000"
        assert result[1] == 12000


class TestPrepareBonusDf(TestBonusLookup):

    def test_should_return_empty_df_when_input_is_none(self):
        lookup = BonusLookup(None, METRIC_TYPE_VOLUMEN)

        assert lookup._bonus_df.empty

    def test_should_return_empty_df_when_input_is_empty(self):
        lookup = BonusLookup(pd.DataFrame(), METRIC_TYPE_VOLUMEN)

        assert lookup._bonus_df.empty

    def test_should_normalize_rut_column(self, sample_bonus_df):
        lookup = BonusLookup(sample_bonus_df, METRIC_TYPE_VOLUMEN)

        assert "RUT_normalized" in lookup._bonus_df.columns
        assert lookup._bonus_df["RUT_normalized"].iloc[0] == "12345678-9"

    def test_should_assign_column_names_when_no_rut_header(self):
        df = pd.DataFrame([
            ["12.345.678-9", "John", 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        ])

        lookup = BonusLookup(df, METRIC_TYPE_VOLUMEN)

        assert "RUT" in lookup._bonus_df.columns
        assert "TCT_Vol" in lookup._bonus_df.columns


class TestGetProductMapping(TestBonusLookup):

    def test_should_return_volumen_mapping_for_volumen_type(self, volumen_lookup):
        mapping = volumen_lookup._get_product_mapping()

        assert "TCT" in mapping
        assert mapping["TCT"] == "TCT_Vol"

    def test_should_include_tctp_in_volumen_mapping(self, volumen_lookup):
        mapping = volumen_lookup._get_product_mapping()

        assert "TCTP" in mapping

    def test_should_return_margen_mapping_for_margen_type(self, margen_lookup):
        mapping = margen_lookup._get_product_mapping()

        assert "TCT" in mapping
        assert mapping["TCT"] == "TCT_Mar"

    def test_should_not_include_tctp_in_margen_mapping(self, margen_lookup):
        mapping = margen_lookup._get_product_mapping()

        assert "TCTP" not in mapping


class TestIsAvailable(TestBonusLookup):

    def test_should_return_true_when_bonus_df_has_data(self, volumen_lookup):
        assert volumen_lookup.is_available() is True

    def test_should_return_false_when_bonus_df_is_empty(self):
        lookup = BonusLookup(pd.DataFrame(), METRIC_TYPE_VOLUMEN)

        assert lookup.is_available() is False


class TestConstants:

    def test_min_compliance_threshold_value(self):
        assert MIN_COMPLIANCE_THRESHOLD == 0.9554

    def test_metric_type_volumen_value(self):
        assert METRIC_TYPE_VOLUMEN == "volumen"

    def test_metric_type_margen_value(self):
        assert METRIC_TYPE_MARGEN == "margen"

    def test_volumen_mapping_contains_expected_products(self):
        assert "TCT" in PRODUCT_BONUS_MAPPING_VOLUMEN
        assert "TAE" in PRODUCT_BONUS_MAPPING_VOLUMEN
        assert "Lubricantes" in PRODUCT_BONUS_MAPPING_VOLUMEN

    def test_margen_mapping_contains_expected_products(self):
        assert "TCT" in PRODUCT_BONUS_MAPPING_MARGEN
        assert "TAE" in PRODUCT_BONUS_MAPPING_MARGEN
        assert "Bluemax TCT" in PRODUCT_BONUS_MAPPING_MARGEN
