from datetime import datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.quarterly_team import (
    CopecQuarterlyTeamBonusStrategy,
)


class TestCopecQuarterlyTeamBonusStrategy:

    @pytest.fixture
    def strategy(self):
        return CopecQuarterlyTeamBonusStrategy(target_period="2025-03-01")

    @pytest.fixture
    def employees_df(self):
        return pd.DataFrame({
            "Rut": ["12.345.678-9", "87.654.321-K", "11.111.111-1", "22.222.222-2"],
            "Nombre": ["Juan Perez", "Maria Lopez", "Pedro Gonzalez", "Ana Martinez"],
            "Jefatura": ["Team Alpha", "Team Alpha", "Team Beta", "Team Beta"],
        })

    @pytest.fixture
    def poa_df(self):
        return pd.DataFrame({
            "Rut": ["12.345.678-9", "87.654.321-K", "11.111.111-1", "22.222.222-2"],
            "Producto": ["TCT (M3)", "TCT (M3)", "TCT (M3)", "TCT (M3)"],
            datetime(2025, 1, 1): [100, 150, 200, 250],
            datetime(2025, 2, 1): [110, 160, 210, 260],
            datetime(2025, 3, 1): [120, 170, 220, 270],
        })

    @pytest.fixture
    def volume_source(self):
        return pd.DataFrame({
            "rut_ejecutivo": ["12345678", "12345678", "12345678",
                             "87654321", "87654321", "87654321",
                             "11111111", "11111111", "11111111",
                             "22222222", "22222222", "22222222"],
            "dv_ejecutivo": ["9", "9", "9", "K", "K", "K", "1", "1", "1", "2", "2", "2"],
            "producto": ["TCT"] * 12,
            "anio": [2025] * 12,
            "mes": [1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3],
            "volumen": [120000, 130000, 140000,
                        160000, 170000, 180000,
                        210000, 220000, 230000,
                        260000, 270000, 280000],
        })

    def _create_data_with_sources(self, sources: dict) -> pd.DataFrame:
        df = pd.DataFrame({"_placeholder": [1]})
        df.attrs["sources"] = sources
        return df


class TestCalculateCommission(TestCopecQuarterlyTeamBonusStrategy):

    def test_should_return_dataframe_with_team_column(
        self, strategy, employees_df, poa_df, volume_source
    ):
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        assert len(result) > 0
        assert "Equipo" in result.columns

    def test_should_group_executives_by_jefatura(
        self, strategy, employees_df, poa_df, volume_source
    ):
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        teams = result["Equipo"].unique()
        assert "Team Alpha" in teams
        assert "Team Beta" in teams
        assert len(teams) == 2

    def test_should_include_required_columns(
        self, strategy, employees_df, poa_df, volume_source
    ):
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        assert "Fecha" in result.columns
        assert "Rep ID" in result.columns
        assert "ID Transaccion" in result.columns
        assert "Equipo" in result.columns
        assert "Miembros" in result.columns
        assert "Bono" in result.columns
        assert "Comision" in result.columns

    def test_should_count_team_members(
        self, strategy, employees_df, poa_df, volume_source
    ):
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        team_alpha = result[result["Equipo"] == "Team Alpha"]
        team_beta = result[result["Equipo"] == "Team Beta"]
        assert team_alpha["Miembros"].iloc[0] == 2
        assert team_beta["Miembros"].iloc[0] == 2

    def test_should_return_empty_dataframe_when_no_sources(self, strategy):
        data = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_dataframe_when_no_employees(self, strategy, poa_df):
        data = self._create_data_with_sources({
            "POA_RESUMEN": poa_df,
        })

        result = strategy.calculate_commission(data)

        assert result.empty


class TestBonusCalculation(TestCopecQuarterlyTeamBonusStrategy):

    def test_should_award_6m_bonus_when_team_meets_poa_all_months(self, strategy):
        employees_df = pd.DataFrame({
            "Rut": ["12.345.678-9"],
            "Nombre": ["Juan Perez"],
            "Jefatura": ["Team Alpha"],
        })
        poa_df = pd.DataFrame({
            "Rut": ["12.345.678-9"],
            "Producto": ["TCT (M3)"],
            datetime(2025, 1, 1): [100],
            datetime(2025, 2, 1): [100],
            datetime(2025, 3, 1): [100],
        })
        volume_source = pd.DataFrame({
            "rut_ejecutivo": ["12345678", "12345678", "12345678"],
            "dv_ejecutivo": ["9", "9", "9"],
            "producto": ["TCT", "TCT", "TCT"],
            "anio": [2025, 2025, 2025],
            "mes": [1, 2, 3],
            "volumen": [150000, 150000, 150000],
        })
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        team_row = result[result["Equipo"] == "Team Alpha"]
        assert team_row["Bono"].iloc[0] == 6_000_000
        assert team_row["Cumple Trimestre"].iloc[0] == "Si"

    def test_should_not_award_bonus_when_team_fails_one_month(self, strategy):
        employees_df = pd.DataFrame({
            "Rut": ["12.345.678-9"],
            "Nombre": ["Juan Perez"],
            "Jefatura": ["Team Alpha"],
        })
        poa_df = pd.DataFrame({
            "Rut": ["12.345.678-9"],
            "Producto": ["TCT (M3)"],
            datetime(2025, 1, 1): [100],
            datetime(2025, 2, 1): [100],
            datetime(2025, 3, 1): [100],
        })
        volume_source = pd.DataFrame({
            "rut_ejecutivo": ["12345678", "12345678", "12345678"],
            "dv_ejecutivo": ["9", "9", "9"],
            "producto": ["TCT", "TCT", "TCT"],
            "anio": [2025, 2025, 2025],
            "mes": [1, 2, 3],
            "volumen": [150000, 150000, 50000],
        })
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        team_row = result[result["Equipo"] == "Team Alpha"]
        assert team_row["Bono"].iloc[0] == 0
        assert team_row["Cumple Trimestre"].iloc[0] == "No"


class TestQuarterMonths(TestCopecQuarterlyTeamBonusStrategy):

    def test_should_get_correct_quarter_months_for_march(self):
        strategy = CopecQuarterlyTeamBonusStrategy(target_period="2025-03-01")

        months = strategy._get_quarter_months()

        assert len(months) == 3
        assert months[0].month == 1
        assert months[1].month == 2
        assert months[2].month == 3

    def test_should_get_correct_quarter_months_for_june(self):
        strategy = CopecQuarterlyTeamBonusStrategy(target_period="2025-06-01")

        months = strategy._get_quarter_months()

        assert len(months) == 3
        assert months[0].month == 4
        assert months[1].month == 5
        assert months[2].month == 6

    def test_should_handle_year_boundary(self):
        strategy = CopecQuarterlyTeamBonusStrategy(target_period="2025-02-01")

        months = strategy._get_quarter_months()

        assert len(months) == 3
        assert months[0].year == 2024
        assert months[0].month == 12
        assert months[1].year == 2025
        assert months[1].month == 1
        assert months[2].year == 2025
        assert months[2].month == 2


class TestJefaturaLookup(TestCopecQuarterlyTeamBonusStrategy):

    def test_should_build_jefatura_lookup_from_employees(self, strategy):
        employees_df = pd.DataFrame({
            "Rut": ["12.345.678-9", "87.654.321-K"],
            "Nombre": ["Juan", "Maria"],
            "Jefatura": ["Team Alpha", "Team Beta"],
        })

        lookup = strategy._build_jefatura_lookup(employees_df)

        assert lookup["12345678-9"] == "Team Alpha"
        assert lookup["87654321-K"] == "Team Beta"

    def test_should_normalize_rut_in_lookup(self, strategy):
        employees_df = pd.DataFrame({
            "Rut": ["12.345.678-9"],
            "Nombre": ["Juan"],
            "Jefatura": ["Team Alpha"],
        })

        lookup = strategy._build_jefatura_lookup(employees_df)

        assert "12345678-9" in lookup

    def test_should_skip_empty_jefatura(self, strategy):
        employees_df = pd.DataFrame({
            "Rut": ["12.345.678-9", "87.654.321-K"],
            "Nombre": ["Juan", "Maria"],
            "Jefatura": ["Team Alpha", ""],
        })

        lookup = strategy._build_jefatura_lookup(employees_df)

        assert len(lookup) == 1
        assert "12345678-9" in lookup


class TestRepIdValue(TestCopecQuarterlyTeamBonusStrategy):

    def test_should_use_equipoventas_as_rep_id(
        self, strategy, employees_df, poa_df, volume_source
    ):
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        assert all(result["Rep ID"] == "EQUIPOVENTAS")


class TestMonthlyColumns(TestCopecQuarterlyTeamBonusStrategy):

    def test_should_include_monthly_columns_in_output(
        self, strategy, employees_df, poa_df, volume_source
    ):
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        assert "Real Ene" in result.columns
        assert "POA Ene" in result.columns
        assert "Cumple Ene" in result.columns
        assert "Real Feb" in result.columns
        assert "POA Feb" in result.columns
        assert "Cumple Feb" in result.columns
        assert "Real Mar" in result.columns
        assert "POA Mar" in result.columns
        assert "Cumple Mar" in result.columns

    def test_should_include_quarterly_totals(
        self, strategy, employees_df, poa_df, volume_source
    ):
        data = self._create_data_with_sources({
            "ejecutivos": employees_df,
            "POA_RESUMEN": poa_df,
            "TCT_TAE": volume_source,
        })

        result = strategy.calculate_commission(data)

        assert "Real Total Q" in result.columns
        assert "POA Total Q" in result.columns
        assert "Meses Cumplidos" in result.columns
