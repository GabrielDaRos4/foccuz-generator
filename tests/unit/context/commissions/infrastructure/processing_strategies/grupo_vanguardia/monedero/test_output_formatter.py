from datetime import datetime

import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
    Brand,
    ConsultantBonus,
    OutputFormatter,
)


class TestOutputFormatter:

    @pytest.fixture
    def formatter(self):
        return OutputFormatter()

    @pytest.fixture
    def sample_bonuses(self):
        return [
            ConsultantBonus(
                consultant_id="1",
                consultant_name="Juan Perez",
                consultant_email="juan@test.com",
                agency="HONDA COLIMA",
                sales_count=5,
                target=4,
                qualifies=True,
                bonus=8000
            ),
            ConsultantBonus(
                consultant_id="2",
                consultant_name="Maria Lopez",
                consultant_email="maria@test.com",
                agency="HONDA VANGUARDIA",
                sales_count=2,
                target=4,
                qualifies=False,
                bonus=0
            )
        ]

    @pytest.fixture
    def qualifying_bonus_only(self):
        return [
            ConsultantBonus(
                consultant_id="1",
                consultant_name="Juan Perez",
                consultant_email="juan@test.com",
                agency="HONDA COLIMA",
                sales_count=5,
                target=4,
                qualifies=True,
                bonus=8000
            )
        ]


class TestFormat(TestOutputFormatter):

    def test_returns_dataframe_with_correct_columns(self, formatter, qualifying_bonus_only):
        result = formatter.format(qualifying_bonus_only, Brand.HONDA, datetime(2024, 12, 1))

        expected_columns = [
            "Fecha", "Rep ID", "ID Transaccion", "Ventas Entregadas", "Meta Ventas",
            "Cumple Meta", "Comision"
        ]
        assert list(result.columns) == expected_columns

    def test_formats_date_correctly(self, formatter, qualifying_bonus_only):
        result = formatter.format(qualifying_bonus_only, Brand.HONDA, datetime(2024, 12, 1))

        assert result["Fecha"].iloc[0] == "2024-12-01"

    def test_generates_transaction_id_with_brand(self, formatter, qualifying_bonus_only):
        result = formatter.format(qualifying_bonus_only, Brand.HONDA, datetime(2024, 12, 1))

        assert result["ID Transaccion"].iloc[0] == "2024-12-01_HONDA_1"

    def test_formats_qualifies_as_si(self, formatter, qualifying_bonus_only):
        result = formatter.format(qualifying_bonus_only, Brand.HONDA, datetime(2024, 12, 1))

        assert result["Cumple Meta"].iloc[0] == "SI"

    def test_includes_column_types_in_attrs(self, formatter, qualifying_bonus_only):
        result = formatter.format(qualifying_bonus_only, Brand.HONDA, datetime(2024, 12, 1))

        assert "column_types" in result.attrs
        assert result.attrs["column_types"]["Comision"] == "money"
        assert result.attrs["column_types"]["Fecha"] == "date"

    def test_returns_empty_dataframe_for_empty_list(self, formatter):
        result = formatter.format([], Brand.HONDA, datetime(2024, 12, 1))

        assert len(result) == 0

    def test_handles_acura_brand(self, formatter, qualifying_bonus_only):
        result = formatter.format(qualifying_bonus_only, Brand.ACURA, datetime(2024, 12, 1))

        assert "ACURA" in result["ID Transaccion"].iloc[0]

    def test_excludes_rows_with_zero_commission(self, formatter, sample_bonuses):
        result = formatter.format(sample_bonuses, Brand.HONDA, datetime(2024, 12, 1))

        assert len(result) == 1
        assert result["Comision"].iloc[0] == 8000

    def test_returns_empty_when_all_commissions_are_zero(self, formatter):
        non_qualifying = [
            ConsultantBonus(
                consultant_id="1",
                consultant_name="Juan",
                consultant_email="juan@test.com",
                agency="HONDA",
                sales_count=1,
                target=4,
                qualifies=False,
                bonus=0
            )
        ]
        result = formatter.format(non_qualifying, Brand.HONDA, datetime(2024, 12, 1))

        assert len(result) == 0
