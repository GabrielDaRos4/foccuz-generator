from datetime import datetime

import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
    CommissionLookup,
)


class TestLookup:

    @pytest.fixture
    def acura_data(self):
        return {
            "mayo_agosto": {
                "INTEGRA ASPEC 2024": {"TABLA 1": 6297, "TABLA 2": 9356, "TABLA 3": 11155},
                "RDX ADVANCE 2025": {"TABLA 1": 6674, "TABLA 2": 9916, "TABLA 3": 11823}
            },
            "septiembre": {
                "ADX ADVANCE 2025": {"TABLA 1": 6370, "TABLA 2": 9464, "TABLA 3": 11284}
            },
            "octubre": {
                "ADX ADVANCE 2025": {"TABLA 1": 5917, "TABLA 2": 8791, "TABLA 3": 10482}
            }
        }

    @pytest.fixture
    def honda_data(self):
        return {
            "mayo": {
                "CR-V EX 2025": {"TABLA 1": 5000, "TABLA 2": 7000, "TABLA 3": 9000}
            },
            "julio": {
                "CIVIC TOURING 2025": {"TABLA 1": 4500, "TABLA 2": 6500, "TABLA 3": 8500}
            },
            "agosto": {
                "HR-V EX 2025": {"TABLA 1": 4000, "TABLA 2": 6000, "TABLA 3": 8000}
            },
            "septiembre": {
                "PILOT TOURING 2025": {"TABLA 1": 8000, "TABLA 2": 12000, "TABLA 3": 15000}
            },
            "octubre": {
                "ACCORD SPORT 2025": {"TABLA 1": 6000, "TABLA 2": 9000, "TABLA 3": 11000}
            },
            "noviembre": {
                "FIT LX 2025": {"TABLA 1": 3000, "TABLA 2": 5000, "TABLA 3": 7000}
            }
        }

    @pytest.fixture
    def lookup(self, acura_data, honda_data):
        lookup = CommissionLookup.__new__(CommissionLookup)
        lookup._acura_tables = acura_data
        lookup._honda_tables = honda_data
        lookup._missing_models = set()
        return lookup

    def test_should_return_commission_for_acura_in_mayo(self, lookup):
        result = lookup.lookup(
            brand="ACURA",
            car_model="INTEGRA ASPEC 2024",
            order_date=datetime(2025, 5, 15),
            commission_type="Tabla 1"
        )
        assert result == 6297

    def test_should_return_commission_for_acura_tabla_3(self, lookup):
        result = lookup.lookup(
            brand="ACURA",
            car_model="RDX ADVANCE 2025",
            order_date=datetime(2025, 6, 20),
            commission_type="Tabla 3"
        )
        assert result == 11823

    def test_should_return_commission_for_honda_in_julio(self, lookup):
        result = lookup.lookup(
            brand="HONDA",
            car_model="CIVIC TOURING 2025",
            order_date=datetime(2025, 7, 10),
            commission_type="Tabla 2"
        )
        assert result == 6500

    def test_should_return_commission_for_honda_in_noviembre(self, lookup):
        result = lookup.lookup(
            brand="HONDA",
            car_model="FIT LX 2025",
            order_date=datetime(2025, 11, 5),
            commission_type="Tabla 1"
        )
        assert result == 3000

    def test_should_return_none_for_unknown_brand(self, lookup):
        result = lookup.lookup(
            brand="TOYOTA",
            car_model="CAMRY",
            order_date=datetime(2025, 7, 1),
            commission_type="Tabla 1"
        )
        assert result is None

    def test_should_return_none_for_unknown_model(self, lookup):
        result = lookup.lookup(
            brand="HONDA",
            car_model="UNKNOWN MODEL",
            order_date=datetime(2025, 7, 1),
            commission_type="Tabla 1"
        )
        assert result is None

    def test_should_return_none_for_date_outside_range(self, lookup):
        result = lookup.lookup(
            brand="HONDA",
            car_model="CR-V EX 2025",
            order_date=datetime(2025, 1, 1),
            commission_type="Tabla 1"
        )
        assert result is None


class TestGetMissingModels:

    @pytest.fixture
    def lookup(self):
        lookup = CommissionLookup.__new__(CommissionLookup)
        lookup._acura_tables = {"mayo_agosto": {"RDX 2025": {"TABLA 1": 5000}}}
        lookup._honda_tables = {"julio": {"CR-V 2025": {"TABLA 1": 4000}}}
        lookup._missing_models = set()
        return lookup

    def test_should_track_missing_model(self, lookup):
        lookup.lookup(
            brand="HONDA",
            car_model="UNKNOWN MODEL",
            order_date=datetime(2025, 7, 15),
            commission_type="Tabla 1"
        )
        missing = lookup.get_missing_models()
        assert len(missing) == 1
        assert ("HONDA", "UNKNOWN MODEL", "2025-07") in missing

    def test_should_not_track_found_model(self, lookup):
        lookup.lookup(
            brand="HONDA",
            car_model="CR-V 2025",
            order_date=datetime(2025, 7, 15),
            commission_type="Tabla 1"
        )
        missing = lookup.get_missing_models()
        assert len(missing) == 0


class TestNormalizeText:

    @pytest.fixture
    def lookup(self):
        lookup = CommissionLookup.__new__(CommissionLookup)
        lookup._acura_tables = {}
        lookup._honda_tables = {}
        lookup._missing_models = set()
        return lookup

    def test_should_normalize_dashes(self, lookup):
        assert lookup._normalize_text("CR–V") == "CR-V"
        assert lookup._normalize_text("CR—V") == "CR-V"

    def test_should_strip_whitespace(self, lookup):
        assert lookup._normalize_text("  CR-V  ") == "CR-V"

    def test_should_uppercase(self, lookup):
        assert lookup._normalize_text("cr-v") == "CR-V"

    def test_should_handle_non_string(self, lookup):
        assert lookup._normalize_text(123) == "123"
