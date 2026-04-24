from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    MONTH_NAMES_ES,
)


class TestMonthNamesES:

    def test_contains_all_12_months(self):
        assert len(MONTH_NAMES_ES) == 12

    def test_january(self):
        assert MONTH_NAMES_ES[1] == "Enero"

    def test_december(self):
        assert MONTH_NAMES_ES[12] == "Diciembre"

    def test_all_months_have_spanish_names(self):
        expected = [
            "Enero", "Febrero", "Marzo", "Abril",
            "Mayo", "Junio", "Julio", "Agosto",
            "Septiembre", "Octubre", "Noviembre", "Diciembre"
        ]
        for i, name in enumerate(expected, 1):
            assert MONTH_NAMES_ES[i] == name
