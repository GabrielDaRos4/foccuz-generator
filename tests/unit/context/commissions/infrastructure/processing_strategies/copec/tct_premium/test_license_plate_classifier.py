from datetime import datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    LicensePlateClassifier,
)


class TestLicensePlateClassifier:

    @pytest.fixture
    def classifier(self):
        return LicensePlateClassifier()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "patente": ["ABC123", "DEF456", "GHI789", "JKL012"],
            "client_rut_complete": ["11111111-1", "22222222-2", "33333333-3", "44444444-4"],
            "volumen_tct_premium": [100, 200, 300, 400],
        })


class TestClassify(TestLicensePlateClassifier):

    def test_new_license_plate_not_in_any_historical(self, classifier, sample_data):
        lp_m1 = set()
        lp_m2 = set()
        lp_historical = set()
        period = datetime(2024, 12, 1)

        result = classifier.classify(sample_data, lp_m1, lp_m2, lp_historical, period)

        assert len(result) == 4
        assert all(result["is_new_license_plate"])

    def test_license_plate_in_m1_is_new(self, classifier, sample_data):
        lp_m1 = {"ABC123"}
        lp_m2 = set()
        lp_historical = set()
        period = datetime(2024, 12, 1)

        result = classifier.classify(sample_data, lp_m1, lp_m2, lp_historical, period)

        lp = result[result["license_plate_normalized"] == "ABC123"]
        assert len(lp) == 1
        assert lp["is_new_license_plate"].iloc[0]

    def test_license_plate_in_m2_is_new(self, classifier, sample_data):
        lp_m1 = set()
        lp_m2 = {"DEF456"}
        lp_historical = set()
        period = datetime(2024, 12, 1)

        result = classifier.classify(sample_data, lp_m1, lp_m2, lp_historical, period)

        lp = result[result["license_plate_normalized"] == "DEF456"]
        assert len(lp) == 1
        assert lp["is_new_license_plate"].iloc[0]

    def test_license_plate_in_historical_excluded(self, classifier, sample_data):
        lp_m1 = set()
        lp_m2 = set()
        lp_historical = {"GHI789"}
        period = datetime(2024, 12, 1)

        result = classifier.classify(sample_data, lp_m1, lp_m2, lp_historical, period)

        assert "GHI789" not in result["license_plate_normalized"].values

    def test_normalizes_license_plate_to_uppercase(self, classifier):
        df = pd.DataFrame({
            "patente": ["abc123"],
            "client_rut_complete": ["11111111-1"],
        })
        lp_m1 = {"ABC123"}
        period = datetime(2024, 12, 1)

        result = classifier.classify(df, lp_m1, set(), set(), period)

        assert result["license_plate_normalized"].iloc[0] == "ABC123"

    def test_returns_only_new_license_plates(self, classifier, sample_data):
        lp_historical = {"ABC123", "DEF456"}
        period = datetime(2024, 12, 1)

        result = classifier.classify(sample_data, set(), set(), lp_historical, period)

        assert len(result) == 2
        assert all(result["is_new_license_plate"])
