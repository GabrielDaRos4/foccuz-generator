import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client import (
    ClientClassifier,
)


class TestClientClassifier:

    @pytest.fixture
    def classifier(self):
        return ClientClassifier()

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "client_rut_complete": ["11111111-1", "22222222-2", "33333333-3", "44444444-4"],
            "volumen": [100, 200, 300, 400],
        })


class TestClassify(TestClientClassifier):

    def test_new_client_with_bonus_not_in_any_historical(self, classifier, sample_data):
        clients_m1 = set()
        clients_m2 = set()
        clients_historical = set()

        result = classifier.classify(sample_data, clients_m1, clients_m2, clients_historical)

        assert len(result) == 4
        assert all(result["is_new_client"])
        assert all(result["gets_bonus"])

    def test_new_client_without_bonus_in_m1(self, classifier, sample_data):
        clients_m1 = {"11111111-1"}
        clients_m2 = set()
        clients_historical = set()

        result = classifier.classify(sample_data, clients_m1, clients_m2, clients_historical)

        client = result[result["client_rut_complete"] == "11111111-1"]
        assert len(client) == 1
        assert client["is_new_client"].iloc[0]
        assert not client["gets_bonus"].iloc[0]

    def test_new_client_without_bonus_in_m2(self, classifier, sample_data):
        clients_m1 = set()
        clients_m2 = {"22222222-2"}
        clients_historical = set()

        result = classifier.classify(sample_data, clients_m1, clients_m2, clients_historical)

        client = result[result["client_rut_complete"] == "22222222-2"]
        assert len(client) == 1
        assert not client["gets_bonus"].iloc[0]

    def test_existing_client_in_historical_excluded(self, classifier, sample_data):
        clients_m1 = set()
        clients_m2 = set()
        clients_historical = {"33333333-3"}

        result = classifier.classify(sample_data, clients_m1, clients_m2, clients_historical)

        assert "33333333-3" not in result["client_rut_complete"].values

    def test_returns_only_new_clients(self, classifier, sample_data):
        clients_m1 = set()
        clients_m2 = set()
        clients_historical = {"11111111-1", "22222222-2"}

        result = classifier.classify(sample_data, clients_m1, clients_m2, clients_historical)

        assert len(result) == 2
        assert all(result["is_new_client"])
