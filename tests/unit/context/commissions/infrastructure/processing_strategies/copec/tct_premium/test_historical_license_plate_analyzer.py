import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    HistoricalLicensePlateAnalyzer,
)


class TestHistoricalLicensePlateAnalyzer:

    @pytest.fixture
    def analyzer(self):
        return HistoricalLicensePlateAnalyzer("TCT PREMIUM")

    @pytest.fixture
    def sample_historical(self):
        return [
            pd.DataFrame({
                "producto": ["TCT PREMIUM", "TCT PREMIUM"],
                "patente": ["ABC123", "DEF456"],
                "volumen_tct_premium": [100, 200],
            }),
            pd.DataFrame({
                "producto": ["TCT PREMIUM", "TCT PREMIUM"],
                "patente": ["GHI789", "JKL012"],
                "volumen_tct_premium": [100, 200],
            }),
            pd.DataFrame({
                "producto": ["TCT PREMIUM", "TCT PREMIUM"],
                "patente": ["MNO345", "PQR678"],
                "volumen_tct_premium": [100, 200],
            }),
        ]


class TestAnalyze(TestHistoricalLicensePlateAnalyzer):

    def test_extracts_license_plates_m1(self, analyzer, sample_historical):
        lp_m1, lp_m2, lp_hist, df_m1, df_m2 = analyzer.analyze(sample_historical)

        assert "ABC123" in lp_m1
        assert "DEF456" in lp_m1
        assert len(lp_m1) == 2

    def test_extracts_license_plates_m2(self, analyzer, sample_historical):
        lp_m1, lp_m2, lp_hist, df_m1, df_m2 = analyzer.analyze(sample_historical)

        assert "GHI789" in lp_m2
        assert "JKL012" in lp_m2
        assert len(lp_m2) == 2

    def test_extracts_historical_license_plates(self, analyzer, sample_historical):
        lp_m1, lp_m2, lp_hist, df_m1, df_m2 = analyzer.analyze(sample_historical)

        assert "MNO345" in lp_hist
        assert "PQR678" in lp_hist

    def test_returns_filtered_dataframes(self, analyzer, sample_historical):
        lp_m1, lp_m2, lp_hist, df_m1, df_m2 = analyzer.analyze(sample_historical)

        assert len(df_m1) == 2
        assert len(df_m2) == 2

    def test_returns_empty_when_no_historical(self, analyzer):
        lp_m1, lp_m2, lp_hist, df_m1, df_m2 = analyzer.analyze([])

        assert len(lp_m1) == 0
        assert len(lp_m2) == 0
        assert len(lp_hist) == 0
        assert df_m1.empty
        assert df_m2.empty

    def test_filters_by_product(self, analyzer):
        historical = [
            pd.DataFrame({
                "producto": ["TCT PREMIUM", "TAE"],
                "patente": ["ABC123", "DEF456"],
                "volumen_tct_premium": [100, 200],
            }),
        ]
        lp_m1, _, _, _, _ = analyzer.analyze(historical)

        assert "ABC123" in lp_m1
        assert "DEF456" not in lp_m1

    def test_filters_zero_volume(self, analyzer):
        historical = [
            pd.DataFrame({
                "producto": ["TCT PREMIUM", "TCT PREMIUM"],
                "patente": ["ABC123", "DEF456"],
                "volumen_tct_premium": [100, 0],
            }),
        ]
        lp_m1, _, _, _, _ = analyzer.analyze(historical)

        assert "ABC123" in lp_m1
        assert "DEF456" not in lp_m1
