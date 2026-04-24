import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.summary.summary_strategy import (
    CopecSummaryStrategy,
)


class TestCopecSummaryStrategy:

    @pytest.fixture
    def strategy(self):
        return CopecSummaryStrategy(target_period="2025-10-01")

    def _create_data_with_sources(self, sources: dict) -> pd.DataFrame:
        df = pd.DataFrame({"_placeholder": [1]})
        df.attrs["sources"] = sources
        return df


class TestCalculateCommission(TestCopecSummaryStrategy):

    def test_should_return_dataframe_with_required_columns(self, strategy):
        sources = {
            "PLAN_800": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [50000],
            })
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert "Fecha" in result.columns
        assert "Rep ID" in result.columns
        assert "Rut" in result.columns
        assert "Ejecutivo" in result.columns
        assert "Comision Clientes Nuevos" in result.columns
        assert "Comision Lubricantes" in result.columns
        assert "Bono Cumplimiento POA" in result.columns
        assert "Total" in result.columns
        assert "Comision" in result.columns

    def test_should_sum_new_client_commissions(self, strategy):
        sources = {
            "PLAN_800": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [50000],
            }),
            "PLAN_806": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [30000],
            }),
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result["Comision Clientes Nuevos"].iloc[0] == 80000

    def test_should_sum_lubricants_commission(self, strategy):
        sources = {
            "PLAN_842": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [25000],
            }),
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert result["Comision Lubricantes"].iloc[0] == 25000

    def test_should_sum_poa_bonus(self, strategy):
        sources = {
            "PLAN_786": pd.DataFrame({
                "Rep ID": ["12345678-9", "12345678-9"],
                "Ejecutivo": ["Juan Perez", "Juan Perez"],
                "Comision": [10000, 15000],
            }),
            "PLAN_856": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [5000],
            }),
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert result["Bono Cumplimiento POA"].iloc[0] == 30000

    def test_should_calculate_total(self, strategy):
        sources = {
            "PLAN_800": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [50000],
            }),
            "PLAN_842": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [25000],
            }),
            "PLAN_786": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [10000],
            }),
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert result["Total"].iloc[0] == 85000
        assert result["Comision"].iloc[0] == 85000

    def test_should_handle_multiple_executives(self, strategy):
        sources = {
            "PLAN_800": pd.DataFrame({
                "Rep ID": ["12345678-9", "87654321-K"],
                "Ejecutivo": ["Juan Perez", "Maria Lopez"],
                "Comision": [50000, 60000],
            }),
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert len(result) == 2

    def test_should_return_empty_when_no_sources(self, strategy):
        data = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_skip_dash_in_comision(self, strategy):
        sources = {
            "PLAN_786": pd.DataFrame({
                "Rep ID": ["12345678-9", "12345678-9"],
                "Ejecutivo": ["Juan Perez", "Juan Perez"],
                "Comision": [10000, "-"],
            }),
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert result["Bono Cumplimiento POA"].iloc[0] == 10000

    def test_should_include_quarterly_team_bonus_column(self, strategy):
        sources = {
            "PLAN_800": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [50000],
            }),
            "ejecutivos": pd.DataFrame({
                "Rut": ["12.345.678-9"],
                "Nombre": ["Juan Perez"],
                "Jefatura": ["Team Alpha"],
            }),
            "PLAN_920": pd.DataFrame({
                "Equipo": ["Team Alpha"],
                "Bono": [6000000],
            }),
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert "Bono Equipo Trimestral" in result.columns
        assert result["Bono Equipo Trimestral"].iloc[0] == 6000000

    def test_should_add_quarterly_team_bonus_to_total(self, strategy):
        sources = {
            "PLAN_800": pd.DataFrame({
                "Rep ID": ["12345678-9"],
                "Ejecutivo": ["Juan Perez"],
                "Comision": [50000],
            }),
            "ejecutivos": pd.DataFrame({
                "Rut": ["12.345.678-9"],
                "Nombre": ["Juan Perez"],
                "Jefatura": ["Team Alpha"],
            }),
            "PLAN_920": pd.DataFrame({
                "Equipo": ["Team Alpha"],
                "Bono": [6000000],
            }),
        }
        data = self._create_data_with_sources(sources)

        result = strategy.calculate_commission(data)

        assert result["Total"].iloc[0] == 6050000


class TestFilterByRepId(TestCopecSummaryStrategy):

    def test_should_use_equipoventas_as_rep_id(self):
        strategy = CopecSummaryStrategy(
            target_period="2025-10-01",
        )
        sources = {
            "PLAN_800": pd.DataFrame({
                "Rep ID": ["12345678-9", "87654321-K"],
                "Ejecutivo": ["Juan Perez", "Maria Lopez"],
                "Comision": [50000, 60000],
            }),
        }
        df = pd.DataFrame({"_placeholder": [1]})
        df.attrs["sources"] = sources

        result = strategy.calculate_commission(df)

        assert len(result) == 2
        assert result["Rep ID"].iloc[0] == "EQUIPOVENTAS"
        assert result["Rep ID"].iloc[1] == "EQUIPOVENTAS"
