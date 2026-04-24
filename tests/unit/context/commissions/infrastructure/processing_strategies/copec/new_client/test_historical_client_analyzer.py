import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client import (
    HistoricalClientAnalyzer,
)


class TestHistoricalClientAnalyzer:

    @pytest.fixture
    def analyzer(self):
        return HistoricalClientAnalyzer("TCT")

    @pytest.fixture
    def sample_historical(self):
        return [
            pd.DataFrame({
                "producto": ["TCT", "TCT"],
                "rut_cliente": ["11111111", "22222222"],
                "dv_cliente": ["1", "2"],
                "volumen": [100, 200],
            }),
            pd.DataFrame({
                "producto": ["TCT", "TCT"],
                "rut_cliente": ["33333333", "44444444"],
                "dv_cliente": ["3", "4"],
                "volumen": [100, 200],
            }),
            pd.DataFrame({
                "producto": ["TCT", "TCT"],
                "rut_cliente": ["55555555", "66666666"],
                "dv_cliente": ["5", "6"],
                "volumen": [100, 200],
            }),
        ]


class TestAnalyze(TestHistoricalClientAnalyzer):

    def test_extracts_clients_m1(self, analyzer, sample_historical):
        clients_m1, clients_m2, clients_hist = analyzer.analyze(sample_historical)

        assert "11111111-1" in clients_m1
        assert "22222222-2" in clients_m1
        assert len(clients_m1) == 2

    def test_extracts_clients_m2(self, analyzer, sample_historical):
        clients_m1, clients_m2, clients_hist = analyzer.analyze(sample_historical)

        assert "33333333-3" in clients_m2
        assert "44444444-4" in clients_m2
        assert len(clients_m2) == 2

    def test_extracts_historical_clients(self, analyzer, sample_historical):
        clients_m1, clients_m2, clients_hist = analyzer.analyze(sample_historical)

        assert "55555555-5" in clients_hist
        assert "66666666-6" in clients_hist

    def test_returns_empty_sets_when_no_historical(self, analyzer):
        clients_m1, clients_m2, clients_hist = analyzer.analyze([])

        assert len(clients_m1) == 0
        assert len(clients_m2) == 0
        assert len(clients_hist) == 0

    def test_handles_single_historical_dataset(self, analyzer, sample_historical):
        clients_m1, clients_m2, clients_hist = analyzer.analyze([sample_historical[0]])

        assert len(clients_m1) == 2
        assert len(clients_m2) == 0
        assert len(clients_hist) == 0

    def test_supports_multiple_product_types(self):
        analyzer = HistoricalClientAnalyzer(["TCT", "TAE"])
        historical = [
            pd.DataFrame({
                "producto": ["TCT", "TAE"],
                "rut_cliente": ["11111111", "22222222"],
                "dv_cliente": ["1", "2"],
                "volumen": [100, 200],
            }),
        ]
        clients_m1, _, _ = analyzer.analyze(historical)

        assert len(clients_m1) == 2
