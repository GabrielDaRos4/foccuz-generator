from datetime import date, datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec.poa_compliance import (
    CopecPoaComplianceStrategy,
)


class TestCopecPoaComplianceStrategy:

    @pytest.fixture
    def strategy(self):
        return CopecPoaComplianceStrategy(target_period="2025-10-01")

    @pytest.fixture
    def tct_tae_source(self):
        # Values in liters (will be converted to M3 by dividing by 1000)
        return pd.DataFrame({
            "rut_ejecutivo": ["12345678", "12345678", "87654321", "87654321"],
            "dv_ejecutivo": ["9", "9", "K", "K"],
            "producto": ["TCT", "TAE", "TCT", "TAE"],
            "volumen": [1000000, 2000000, 500000, 800000],  # 1000, 2000, 500, 800 M3
            "contribucion": [50000000, 100000000, 25000000, 40000000],  # 50, 50, 50, 50 $/L
        })

    @pytest.fixture
    def cupon_electronico_source(self):
        # Values in liters (will be converted to M3 by dividing by 1000)
        return pd.DataFrame({
            "rut_ejecutivo": ["12345678"],
            "dv_ejecutivo": ["9"],
            "producto": ["Cupon Electronico"],
            "volumen": [500000],  # 500 M3
            "contribucion": [25000000],
        })

    @pytest.fixture
    def app_copec_source(self):
        # Values in liters (will be converted to M3 by dividing by 1000)
        return pd.DataFrame({
            "rut_ejecutivo": ["12345678", "12345678"],
            "dv_ejecutivo": ["9", "9"],
            "producto": ["App Copec Empresa Combustible", "App Copec Empresa Bluemax"],
            "volumen": [300000, 150000],  # 300, 150 M3
        })

    def _create_data_with_sources(self, sources: dict) -> pd.DataFrame:
        df = pd.DataFrame({"_placeholder": [1]})
        df.attrs["sources"] = sources
        return df


class TestCalculateCommission(TestCopecPoaComplianceStrategy):

    def test_should_return_dataframe_with_rep_id_when_sources_provided(
        self, strategy, tct_tae_source
    ):
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        assert len(result) > 0
        assert "Rep ID" in result.columns

    def test_should_return_all_products_per_rep(self, strategy, tct_tae_source):
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        rep1_rows = result[result["Rep ID"] == "12345678-9"]
        assert len(rep1_rows) == 10

    def test_should_include_required_columns(self, strategy, tct_tae_source):
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        assert "Fecha" in result.columns
        assert "Rep ID" in result.columns
        assert "ID Transaccion" in result.columns
        assert "Producto" in result.columns
        assert "Real Oct 2025" in result.columns
        assert "POA Oct 2025" in result.columns

    def test_should_calculate_tct_volume_per_rep(self, strategy, tct_tae_source):
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        tct_row = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "TCT (M3)")]
        assert len(tct_row) == 1
        assert tct_row["Real Oct 2025"].iloc[0] == 1000  # 1000000 liters / 1000 = 1000 M3

    def test_should_calculate_margin_per_product(self):
        strategy = CopecPoaComplianceStrategy(target_period="2025-10-01", metric_type="margen")
        tct_tae_source = pd.DataFrame({
            "rut_ejecutivo": ["12345678", "12345678"],
            "dv_ejecutivo": ["9", "9"],
            "producto": ["TCT", "TAE"],
            "volumen": [1000000, 2000000],
            "contribucion": [50000000, 100000000],
        })
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        tct_row = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "TCT ($/M3)")]
        assert tct_row["Real Oct 2025"].iloc[0] == 50.0

    def test_should_create_ce_plus_appce_combined_row(
        self, strategy, cupon_electronico_source, app_copec_source
    ):
        data = self._create_data_with_sources({
            "CUPON_ELECTRONICO": cupon_electronico_source,
            "APP_COPEC": app_copec_source,
        })

        result = strategy.calculate_commission(data)

        combined_row = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "CE + AppCE (M3)")]
        assert len(combined_row) == 1
        assert combined_row["Real Oct 2025"].iloc[0] == 800  # 500000 + 300000 = 800000 liters / 1000 = 800 M3

    def test_should_add_date_column_with_target_period(self, strategy, tct_tae_source):
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        assert "Fecha" in result.columns
        assert result["Fecha"].iloc[0] == "2025-10-01"

    def test_should_generate_transaction_id_with_product(self, strategy, tct_tae_source):
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        tct_row = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "TCT (M3)")]
        assert "TCT" in tct_row["ID Transaccion"].iloc[0]

    def test_should_return_empty_dataframe_when_no_sources(self, strategy):
        data = pd.DataFrame()

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_return_empty_dataframe_when_sources_attr_empty(self, strategy):
        data = self._create_data_with_sources({})

        result = strategy.calculate_commission(data)

        assert result.empty

    def test_should_include_all_products_even_with_zero_volume(self, strategy):
        source = pd.DataFrame({
            "rut_ejecutivo": ["12345678", "12345678"],
            "dv_ejecutivo": ["9", "9"],
            "producto": ["TCT", "TAE"],
            "volumen": [1000, 0],
            "contribucion": [50000, 0],
        })
        data = self._create_data_with_sources({"TCT_TAE": source})

        result = strategy.calculate_commission(data)

        rep_rows = result[result["Rep ID"] == "12345678-9"]
        products = rep_rows["Producto"].tolist()
        assert "TCT (M3)" in products
        assert "TAE (M3)" in products
        tae_row = rep_rows[rep_rows["Producto"] == "TAE (M3)"]
        assert tae_row["Real Oct 2025"].iloc[0] == 0


class TestFilterByRepId(TestCopecPoaComplianceStrategy):

    def test_should_filter_results_by_rep_id(self, tct_tae_source):
        strategy = CopecPoaComplianceStrategy(
            target_period="2025-10-01",
            rep_id_filter="12345678-9"
        )
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        assert all(result["Rep ID"] == "12345678-9")


class TestExtractPeriod(TestCopecPoaComplianceStrategy):

    def test_should_parse_string_period(self):
        strategy = CopecPoaComplianceStrategy(target_period="2025-10-01")

        result = strategy._extract_period()

        assert result.year == 2025
        assert result.month == 10
        assert result.day == 1

    def test_should_handle_datetime_object(self):
        dt = datetime(2025, 10, 1)
        strategy = CopecPoaComplianceStrategy(target_period=dt)

        result = strategy._extract_period()

        assert result.year == 2025
        assert result.month == 10

    def test_should_handle_date_object(self):
        d = date(2025, 10, 1)
        strategy = CopecPoaComplianceStrategy(target_period=d)

        result = strategy._extract_period()

        assert result.year == 2025
        assert result.month == 10

    def test_should_return_current_month_when_none(self):
        strategy = CopecPoaComplianceStrategy(target_period=None)

        result = strategy._extract_period()

        assert result.day == 1


class TestRepIdExtraction(TestCopecPoaComplianceStrategy):

    def test_should_extract_rep_id_with_dv(self, strategy):
        source = pd.DataFrame({
            "rut_ejecutivo": ["12345678"],
            "dv_ejecutivo": ["9"],
            "producto": ["TCT"],
            "volumen": [1000],
        })

        rep_ids = strategy._extract_rep_ids_from_df(
            strategy._normalize_columns(source)
        )

        assert "12345678-9" in rep_ids

    def test_should_skip_sin_informacion_reps(self, strategy):
        source = pd.DataFrame({
            "rut_ejecutivo": ["12345678", "SIN_INFORMACION"],
            "dv_ejecutivo": ["9", "X"],
            "producto": ["TCT", "TCT"],
            "volumen": [1000, 500],
        })

        rep_ids = strategy._extract_rep_ids_from_df(
            strategy._normalize_columns(source)
        )

        assert "12345678-9" in rep_ids
        assert len(rep_ids) == 1

    def test_should_handle_uppercase_columns(self, strategy):
        source = pd.DataFrame({
            "RUT_EJECUTIVO": ["12345678"],
            "DV_EJECUTIVO": ["9"],
            "PRODUCTO": ["TCT"],
            "VOLUMEN": [1000000],  # Liters
        })
        data = self._create_data_with_sources({"TCT_TAE": source})

        result = strategy.calculate_commission(data)

        assert len(result) == 10
        tct_row = result[result["Producto"] == "TCT (M3)"]
        assert tct_row["Real Oct 2025"].iloc[0] == 1000  # 1000000 liters / 1000 = 1000 M3


class TestProductOrdering(TestCopecPoaComplianceStrategy):

    def test_should_order_products_within_rep(self, strategy, tct_tae_source):
        data = self._create_data_with_sources({"TCT_TAE": tct_tae_source})

        result = strategy.calculate_commission(data)

        rep_rows = result[result["Rep ID"] == "12345678-9"]
        products = rep_rows["Producto"].tolist()
        assert products.index("TCT (M3)") < products.index("TAE (M3)")


class TestTctpPatentCounting(TestCopecPoaComplianceStrategy):

    def test_should_count_unique_patents_per_rep_for_tctp(self, strategy):
        tct_premium_data = pd.DataFrame({
            "anio": [2025, 2025, 2025, 2025, 2025],
            "mes": [10, 10, 10, 10, 10],
            "producto": ["TCT Premium", "TCT Premium", "TCT Premium", "TCT Premium", "TCT Premium"],
            "rut_ejecutivo": ["12345678", "12345678", "12345678", "87654321", "87654321"],
            "dv_ejecutivo": ["9", "9", "9", "K", "K"],
            "patente": ["ABC123", "DEF456", "ABC123", "GHI789", "JKL012"],
            "volumen_tct_premium": [100, 200, 150, 300, 400],
        })
        data = self._create_data_with_sources({"TCT_PREMIUM": tct_premium_data})

        result = strategy.calculate_commission(data)

        tctp_rep1 = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "TCTP (N patentes)")]
        tctp_rep2 = result[(result["Rep ID"] == "87654321-K") & (result["Producto"] == "TCTP (N patentes)")]
        assert tctp_rep1["Real Oct 2025"].iloc[0] == 2
        assert tctp_rep2["Real Oct 2025"].iloc[0] == 2

    def test_should_count_patents_with_uppercase_producto(self, strategy):
        tct_premium_data = pd.DataFrame({
            "anio": [2025, 2025, 2025],
            "mes": [10, 10, 10],
            "producto": ["TCT PREMIUM", "TCT PREMIUM", "TCT PREMIUM"],
            "rut_ejecutivo": ["12345678", "12345678", "12345678"],
            "dv_ejecutivo": ["9", "9", "9"],
            "patente": ["AAA111", "BBB222", "CCC333"],
            "volumen_tct_premium": [500, 600, 700],
        })
        data = self._create_data_with_sources({"TCT_PREMIUM": tct_premium_data})

        result = strategy.calculate_commission(data)

        tctp_row = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "TCTP (N patentes)")]
        assert tctp_row["Real Oct 2025"].iloc[0] == 3

    def test_should_return_zero_when_patente_column_missing(self, strategy):
        tct_premium_data = pd.DataFrame({
            "anio": [2025, 2025],
            "mes": [10, 10],
            "producto": ["TCT Premium", "TCT Premium"],
            "rut_ejecutivo": ["12345678", "12345678"],
            "dv_ejecutivo": ["9", "9"],
            "volumen_tct_premium": [100, 200],
        })
        data = self._create_data_with_sources({"TCT_PREMIUM": tct_premium_data})

        result = strategy.calculate_commission(data)

        tctp_row = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "TCTP (N patentes)")]
        assert tctp_row["Real Oct 2025"].iloc[0] == 0

    def test_should_filter_patents_by_target_period(self, strategy):
        tct_premium_data = pd.DataFrame({
            "anio": [2025, 2025, 2025, 2025, 2025],
            "mes": [10, 10, 10, 9, 9],
            "producto": ["TCT Premium"] * 5,
            "rut_ejecutivo": ["12345678"] * 5,
            "dv_ejecutivo": ["9"] * 5,
            "patente": ["AAA111", "BBB222", "CCC333", "DDD444", "EEE555"],
            "volumen_tct_premium": [100, 200, 300, 400, 500],
        })
        data = self._create_data_with_sources({"TCT_PREMIUM": tct_premium_data})

        result = strategy.calculate_commission(data)

        tctp_row = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "TCTP (N patentes)")]
        assert tctp_row["Real Oct 2025"].iloc[0] == 3

    def test_should_only_count_patents_with_positive_volume(self, strategy):
        tct_premium_data = pd.DataFrame({
            "anio": [2025, 2025, 2025, 2025, 2025],
            "mes": [10, 10, 10, 10, 10],
            "producto": ["TCT Premium"] * 5,
            "rut_ejecutivo": ["12345678"] * 5,
            "dv_ejecutivo": ["9"] * 5,
            "patente": ["AAA111", "BBB222", "CCC333", "DDD444", "EEE555"],
            "volumen_tct_premium": [100, 200, 0, 0, 0],
        })
        data = self._create_data_with_sources({"TCT_PREMIUM": tct_premium_data})

        result = strategy.calculate_commission(data)

        tctp_row = result[(result["Rep ID"] == "12345678-9") & (result["Producto"] == "TCTP (N patentes)")]
        assert tctp_row["Real Oct 2025"].iloc[0] == 2
