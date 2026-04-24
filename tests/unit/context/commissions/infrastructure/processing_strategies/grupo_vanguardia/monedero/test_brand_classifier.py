import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
    Brand,
    BrandClassifier,
)


class TestBrandClassifier:

    @pytest.fixture
    def classifier(self):
        return BrandClassifier()

    @pytest.mark.parametrize("model,expected", [
        ("CR-V", Brand.HONDA),
        ("HR-V", Brand.HONDA),
        ("CIVIC", Brand.HONDA),
        ("CITY", Brand.HONDA),
        ("FIT", Brand.HONDA),
        ("BR-V", Brand.HONDA),
        ("PILOT", Brand.HONDA),
        ("ACCORD", Brand.HONDA),
        ("ODYSSEY", Brand.HONDA),
        ("PASSPORT", Brand.HONDA),
    ])
    def test_classifies_honda_models(self, classifier, model, expected):
        assert classifier.classify(model) == expected

    @pytest.mark.parametrize("model,expected", [
        ("RDX", Brand.ACURA),
        ("MDX", Brand.ACURA),
        ("ILX", Brand.ACURA),
        ("TLX", Brand.ACURA),
        ("ILX TECH", Brand.ACURA),
        ("NSX", Brand.ACURA),
        ("RLX", Brand.ACURA),
        ("ACURA", Brand.ACURA),
    ])
    def test_classifies_acura_models(self, classifier, model, expected):
        assert classifier.classify(model) == expected

    def test_classifies_acura_when_model_contains_acura(self, classifier):
        assert classifier.classify("ACURA RDX") == Brand.ACURA
        assert classifier.classify("ACURA MDX") == Brand.ACURA

    def test_handles_lowercase_input(self, classifier):
        assert classifier.classify("cr-v") == Brand.HONDA
        assert classifier.classify("rdx") == Brand.ACURA

    def test_handles_mixed_case_input(self, classifier):
        assert classifier.classify("Cr-V") == Brand.HONDA
        assert classifier.classify("Rdx") == Brand.ACURA

    def test_handles_whitespace(self, classifier):
        assert classifier.classify("  CR-V  ") == Brand.HONDA
        assert classifier.classify("  RDX  ") == Brand.ACURA

    def test_returns_unknown_for_empty_string(self, classifier):
        assert classifier.classify("") == Brand.UNKNOWN

    def test_returns_unknown_for_none(self, classifier):
        assert classifier.classify(None) == Brand.UNKNOWN

    def test_returns_unknown_for_unrecognized_model(self, classifier):
        assert classifier.classify("TESLA") == Brand.UNKNOWN
        assert classifier.classify("CAMRY") == Brand.UNKNOWN
