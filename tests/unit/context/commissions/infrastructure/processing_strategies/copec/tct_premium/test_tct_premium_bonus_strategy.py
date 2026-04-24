import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    TctPremiumBonusStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.copec.tct_premium import (
    tct_premium_bonus_strategy as _tpbs_mod,
)

_combine_months = _tpbs_mod._combine_months
_extract_license_plates = _tpbs_mod._extract_license_plates
_normalize_columns = _tpbs_mod._normalize_columns


class TestTctPremiumBonusStrategy:

    @pytest.fixture
    def strategy(self):
        return TctPremiumBonusStrategy(
            product_type="TCT PREMIUM",
            bonus_per_month=15000,
            target_period="2025-01-01"
        )

    @pytest.fixture
    def sample_sales(self):
        return pd.DataFrame({
            'Ejecutivo': ['1234567890', '1234567890'],
            'Patente': ['ABC123', 'DEF456'],
            'Producto': ['TCT PREMIUM', 'TCT PREMIUM'],
            'Volumen_TCT_Premium': [100, 200],
            'Periodo': ['2025-01-01', '2025-01-01'],
        })

    @pytest.fixture
    def sample_historical(self):
        return [
            pd.DataFrame({
                'Ejecutivo': ['1234567890'],
                'Patente': ['GHI789'],
                'Producto': ['TCT PREMIUM'],
                'Volumen_TCT_Premium': [50],
                'Periodo': ['2024-12-01'],
            }),
        ]


class TestCalculateCommission(TestTctPremiumBonusStrategy):

    def test_should_return_empty_dataframe_when_data_is_empty(self, strategy):
        data = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_no_product_matches(self, strategy):
        data = pd.DataFrame({
            'Ejecutivo': ['1234567890'],
            'Patente': ['ABC123'],
            'Producto': ['TAE'],
            'Volumen_TCT_Premium': [100],
            'Periodo': ['2025-01-01'],
        })
        data.attrs['ventas_historicas'] = []

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_when_volume_is_zero(self, strategy):
        data = pd.DataFrame({
            'Ejecutivo': ['1234567890'],
            'Patente': ['ABC123'],
            'Producto': ['TCT PREMIUM'],
            'Volumen_TCT_Premium': [0],
            'Periodo': ['2025-01-01'],
        })
        data.attrs['ventas_historicas'] = []

        result = strategy.calculate_commission(data)

        assert result.empty


class TestFilterProduct(TestTctPremiumBonusStrategy):

    def test_should_filter_by_product_type(self, strategy):
        data = pd.DataFrame({
            'producto': ['TCT PREMIUM', 'TAE', 'TCT PREMIUM'],
            'patente': ['ABC123', 'DEF456', 'GHI789'],
        })

        result = strategy._filter_product(data)

        assert len(result) == 2
        assert all(result['producto'] == 'TCT PREMIUM')

    def test_should_return_all_rows_when_no_product_column(self, strategy):
        data = pd.DataFrame({
            'patente': ['ABC123', 'DEF456'],
        })

        result = strategy._filter_product(data)

        assert len(result) == 2


class TestFilterPositiveVolume(TestTctPremiumBonusStrategy):

    def test_should_filter_positive_volumes(self, strategy):
        data = pd.DataFrame({
            'volumen_tct_premium': [100, 0, -50, 200],
            'patente': ['A', 'B', 'C', 'D'],
        })

        result = strategy._filter_positive_volume(data)

        assert len(result) == 2
        assert all(result['volumen_tct_premium'] > 0)

    def test_should_return_all_when_no_volume_column(self, strategy):
        data = pd.DataFrame({
            'patente': ['ABC123', 'DEF456'],
        })

        result = strategy._filter_positive_volume(data)

        assert len(result) == 2


class TestFilterByRepId(TestTctPremiumBonusStrategy):

    def test_should_filter_by_rep_id(self, strategy):
        strategy._rep_id_filter = "1234567890"
        data = pd.DataFrame({
            'ejecutivo': ['1234567890', '9999999999'],
            'patente': ['ABC123', 'DEF456'],
        })

        result = strategy._filter_by_rep_id(data)

        assert len(result) == 1


class TestGetColumnTypes(TestTctPremiumBonusStrategy):

    def test_should_return_column_types_dict(self, strategy):
        column_types = strategy.get_column_types()

        assert isinstance(column_types, dict)
        assert len(column_types) > 0


class TestExtractLicensePlates:

    def test_should_extract_unique_license_plates(self):
        df = pd.DataFrame({
            'patente': ['ABC123', 'DEF456', 'ABC123'],
        })

        result = _extract_license_plates(df)

        assert result == {'ABC123', 'DEF456'}

    def test_should_normalize_to_uppercase(self):
        df = pd.DataFrame({
            'patente': ['abc123', 'Def456'],
        })

        result = _extract_license_plates(df)

        assert result == {'ABC123', 'DEF456'}

    def test_should_return_empty_set_when_no_patente_column(self):
        df = pd.DataFrame({
            'other_col': ['value'],
        })

        result = _extract_license_plates(df)

        assert result == set()


class TestNormalizeColumns:

    def test_should_convert_columns_to_lowercase(self):
        df = pd.DataFrame({
            'PATENTE': ['ABC123'],
            'Producto': ['TCT'],
        })

        result = _normalize_columns(df)

        assert 'patente' in result.columns
        assert 'producto' in result.columns

    def test_should_strip_whitespace(self):
        df = pd.DataFrame({
            '  patente  ': ['ABC123'],
        })

        result = _normalize_columns(df)

        assert 'patente' in result.columns


class TestCombineMonths:

    def test_should_combine_three_months(self):
        df_m0 = pd.DataFrame({'patente': ['A']})
        df_m1 = pd.DataFrame({'patente': ['B']})
        df_m2 = pd.DataFrame({'patente': ['C']})

        result = _combine_months(df_m0, df_m1, df_m2)

        assert len(result) == 3
        assert '_month_offset' in result.columns

    def test_should_set_month_offsets_correctly(self):
        df_m0 = pd.DataFrame({'patente': ['A']})
        df_m1 = pd.DataFrame({'patente': ['B']})
        df_m2 = pd.DataFrame({'patente': ['C']})

        result = _combine_months(df_m0, df_m1, df_m2)

        assert result[result['patente'] == 'A']['_month_offset'].iloc[0] == 0
        assert result[result['patente'] == 'B']['_month_offset'].iloc[0] == 1
        assert result[result['patente'] == 'C']['_month_offset'].iloc[0] == 2

    def test_should_return_empty_when_all_empty(self):
        result = _combine_months(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

        assert result.empty

    def test_should_handle_partial_data(self):
        df_m0 = pd.DataFrame({'patente': ['A']})

        result = _combine_months(df_m0, pd.DataFrame(), pd.DataFrame())

        assert len(result) == 1
