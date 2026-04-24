from datetime import date, datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.lubricants import (
    CopecLubricantsCommissionStrategy,
)


class TestCopecLubricantsCommissionStrategy:

    @pytest.fixture
    def strategy(self):
        return CopecLubricantsCommissionStrategy(target_period="2025-10-01")

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            "Rut": ["12.345.678-9", "98.765.432-1"],
            "Vendedor": ["Juan Perez", "Maria Lopez"],
            "Cliente": ["Cliente A", "Cliente B"],
            "Solicitante": ["Sol A", "Sol B"],
            "Vol. 2025": [100, 200],
            "DESC-PL 2025": [0.05, 0.08],
            "Comision $": [5000, 10000],
        })


class TestCalculateCommission(TestCopecLubricantsCommissionStrategy):

    def test_returns_dataframe_with_commissions(self, strategy, sample_data):
        result = strategy.calculate_commission(sample_data)

        assert len(result) == 2
        assert "Comision" in result.columns

    def test_calculates_commission_per_liter(self, strategy, sample_data):
        result = strategy.calculate_commission(sample_data)

        assert "Comision $/L" in result.columns
        assert result["Comision $/L"].iloc[0] == 50.0  # 5000 / 100

    def test_adds_date_column(self, strategy, sample_data):
        result = strategy.calculate_commission(sample_data)

        assert "Fecha" in result.columns
        assert result["Fecha"].iloc[0] == "2025-10-01"

    def test_builds_rep_id_from_rut(self, strategy, sample_data):
        result = strategy.calculate_commission(sample_data)

        assert "Rep ID" in result.columns
        assert result["Rep ID"].iloc[0] == "12345678-9"

    def test_returns_empty_when_data_empty(self, strategy):
        result = strategy.calculate_commission(pd.DataFrame())

        assert result.empty

    def test_returns_empty_when_solicitante_missing(self, strategy):
        data = pd.DataFrame({
            "Rut": ["12.345.678-9"],
            "Vol. 2025": [100],
        })

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_filters_rows_with_zero_volume(self, strategy):
        data = pd.DataFrame({
            "Rut": ["12.345.678-9", "98.765.432-1"],
            "Solicitante": ["Sol A", "Sol B"],
            "Vol. 2025": [0, 200],
            "Comision $": [5000, 10000],
        })

        result = strategy.calculate_commission(data)

        assert len(result) == 1


class TestExtractPeriod(TestCopecLubricantsCommissionStrategy):

    def test_parses_string_period(self):
        strategy = CopecLubricantsCommissionStrategy(target_period="2025-10-01")
        result = strategy._extract_period()

        assert result.year == 2025
        assert result.month == 10
        assert result.day == 1

    def test_handles_datetime_object(self):
        dt = datetime(2025, 10, 1)
        strategy = CopecLubricantsCommissionStrategy(target_period=dt)
        result = strategy._extract_period()

        assert result.year == 2025
        assert result.month == 10

    def test_handles_date_object(self):
        d = date(2025, 10, 1)
        strategy = CopecLubricantsCommissionStrategy(target_period=d)
        result = strategy._extract_period()

        assert result.year == 2025
        assert result.month == 10

    def test_returns_current_month_when_none(self):
        strategy = CopecLubricantsCommissionStrategy(target_period=None)
        result = strategy._extract_period()

        assert result.day == 1

    def test_returns_current_month_on_invalid_format(self):
        strategy = CopecLubricantsCommissionStrategy(target_period="invalid")
        result = strategy._extract_period()

        assert result.day == 1


class TestFilterByRepId(TestCopecLubricantsCommissionStrategy):

    def test_filters_by_rep_id(self):
        strategy = CopecLubricantsCommissionStrategy(
            target_period="2025-10-01",
            rep_id_filter="12345678-9"
        )
        data = pd.DataFrame({
            "Rut": ["12.345.678-9", "98.765.432-1"],
            "Solicitante": ["Sol A", "Sol B"],
            "Vol. 2025": [100, 200],
            "Comision $": [5000, 10000],
        })

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result["Rep ID"].iloc[0] == "12345678-9"
