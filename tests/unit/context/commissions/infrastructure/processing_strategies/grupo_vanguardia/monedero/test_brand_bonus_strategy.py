import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.grupo_vanguardia.monedero import (
    BrandBonusStrategy,
    grupo_vanguardia_sales_merge,
)


class TestHondaBrandBonusStrategy:

    @pytest.fixture
    def strategy(self):
        return BrandBonusStrategy(brand="HONDA", min_sales=4, bonus_amount=8000, target_period="2024-12")

    @pytest.fixture
    def sample_sales(self):
        return pd.DataFrame({
            "Model": ["CR-V", "CR-V", "CR-V", "CR-V", "HR-V", "HR-V", "RDX"],
            "Brand": ["HONDA", "HONDA", "HONDA", "HONDA", "HONDA", "HONDA", "ACURA"],
            "Status": ["Entregado"] * 7,
            "Delivery_Date": ["2024-12-01"] * 7,
            "IdConsultant": [1, 1, 1, 1, 2, 2, 3],
            "Consultant_Name": ["Juan"] * 4 + ["Maria"] * 2 + ["Pedro"],
            "Consultant_Mail": ["juan@test.com"] * 4 + ["maria@test.com"] * 2 + ["pedro@test.com"],
            "Agency": ["HONDA COLIMA"] * 7
        })

    def test_calculates_bonus_for_consultant_with_4_sales(self, strategy, sample_sales):
        result = strategy.calculate_commission(sample_sales)

        juan = result[result["Rep ID"] == "1"]
        assert len(juan) == 1
        assert juan["Ventas Entregadas"].iloc[0] == 4
        assert juan["Cumple Meta"].iloc[0] == "SI"
        assert juan["Comision"].iloc[0] == 8000

    def test_excludes_consultant_below_target(self, strategy, sample_sales):
        result = strategy.calculate_commission(sample_sales)

        maria = result[result["Rep ID"] == "2"]
        assert len(maria) == 0

    def test_excludes_acura_models(self, strategy, sample_sales):
        result = strategy.calculate_commission(sample_sales)

        pedro = result[result["Rep ID"] == "3"]
        assert len(pedro) == 0

    def test_returns_empty_for_empty_dataframe(self, strategy):
        result = strategy.calculate_commission(pd.DataFrame())
        assert len(result) == 0

    def test_filters_by_target_period(self, strategy):
        sales = pd.DataFrame({
            "Model": ["CR-V", "CR-V", "CR-V", "CR-V", "CR-V"],
            "Brand": ["HONDA"] * 5,
            "Status": ["Entregado"] * 5,
            "Delivery_Date": ["2024-11-01", "2024-12-01", "2024-12-15", "2024-12-20", "2024-12-25"],
            "IdConsultant": [1, 1, 1, 1, 1],
            "Consultant_Name": ["Juan"] * 5,
            "Consultant_Mail": ["juan@test.com"] * 5,
            "Agency": ["HONDA COLIMA"] * 5
        })

        result = strategy.calculate_commission(sales)

        assert len(result) == 1
        assert result["Ventas Entregadas"].iloc[0] == 4

    def test_excludes_non_delivered_sales(self, strategy):
        sales = pd.DataFrame({
            "Model": ["CR-V", "CR-V", "CR-V", "CR-V"],
            "Brand": ["HONDA"] * 4,
            "Status": ["Entregado", "Facturado", "Entregado", "Entregado"],
            "Delivery_Date": ["2024-12-01"] * 4,
            "IdConsultant": [1, 1, 1, 1],
            "Consultant_Name": ["Juan"] * 4,
            "Consultant_Mail": ["juan@test.com"] * 4,
            "Agency": ["HONDA COLIMA"] * 4
        })

        result = strategy.calculate_commission(sales)

        assert len(result) == 0


class TestAcuraBrandBonusStrategy:

    @pytest.fixture
    def strategy(self):
        return BrandBonusStrategy(brand="ACURA", min_sales=3, bonus_amount=8000, target_period="2024-12")

    @pytest.fixture
    def sample_sales(self):
        return pd.DataFrame({
            "Model": ["RDX", "RDX", "RDX", "MDX", "CR-V"],
            "Brand": ["ACURA", "ACURA", "ACURA", "ACURA", "HONDA"],
            "Status": ["Entregado"] * 5,
            "Delivery_Date": ["2024-12-01"] * 5,
            "IdConsultant": [1, 1, 1, 2, 3],
            "Consultant_Name": ["Juan"] * 3 + ["Maria", "Pedro"],
            "Consultant_Mail": ["juan@test.com"] * 3 + ["maria@test.com", "pedro@test.com"],
            "Agency": ["HONDA COLIMA"] * 5
        })

    def test_calculates_bonus_for_consultant_with_3_sales(self, strategy, sample_sales):
        result = strategy.calculate_commission(sample_sales)

        juan = result[result["Rep ID"] == "1"]
        assert len(juan) == 1
        assert juan["Ventas Entregadas"].iloc[0] == 3
        assert juan["Cumple Meta"].iloc[0] == "SI"
        assert juan["Comision"].iloc[0] == 8000

    def test_excludes_consultant_below_target(self, strategy, sample_sales):
        result = strategy.calculate_commission(sample_sales)

        maria = result[result["Rep ID"] == "2"]
        assert len(maria) == 0

    def test_excludes_honda_models(self, strategy, sample_sales):
        result = strategy.calculate_commission(sample_sales)

        pedro = result[result["Rep ID"] == "3"]
        assert len(pedro) == 0


class TestGrupoVanguardiaSalesMerge:

    def test_returns_sales_dataframe(self):
        sales = pd.DataFrame({"col": [1, 2, 3]})
        consultants = pd.DataFrame({"col": ["a", "b"]})

        result = grupo_vanguardia_sales_merge(
            {"sales": sales, "consultants": consultants},
            {}
        )

        assert len(result) == 3

    def test_attaches_consultants_to_attrs(self):
        sales = pd.DataFrame({"col": [1, 2, 3]})
        consultants = pd.DataFrame({"col": ["a", "b"]})

        result = grupo_vanguardia_sales_merge(
            {"sales": sales, "consultants": consultants},
            {}
        )

        assert "consultants" in result.attrs
        assert len(result.attrs["consultants"]) == 2

    def test_raises_error_when_no_sales(self):
        with pytest.raises(ValueError, match="Sales dataframe is required"):
            grupo_vanguardia_sales_merge({"consultants": pd.DataFrame()}, {})

    def test_handles_missing_consultants(self):
        sales = pd.DataFrame({"col": [1, 2, 3]})

        result = grupo_vanguardia_sales_merge({"sales": sales}, {})

        assert len(result) == 3
