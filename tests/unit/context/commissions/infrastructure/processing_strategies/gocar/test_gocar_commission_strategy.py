from datetime import datetime

import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.gocar import (
    GocarCommissionStrategy,
)


class TestGocarCommissionStrategy:

    @pytest.fixture
    def strategy_nuevos(self):
        return GocarCommissionStrategy(department_type="NUEVOS")

    @pytest.fixture
    def strategy_seminuevos(self):
        return GocarCommissionStrategy(department_type="SEMINUEVOS")

    @pytest.fixture
    def users_mapping(self):
        return pd.DataFrame({
            "Rep ID": [170865, 170862, 170864],
            "Nombre": ["KARLA RUIZ", "AIMME AGUILAR", "JONATHAN CEDENO"]
        })

    @pytest.fixture
    def sales_data(self):
        return pd.DataFrame({
            "  AGENTE O VENDEDOR": ["KARLA RUIZ PALACIOS", "AIMME CHRISTIAN AGUILAR VAZQUEZ"],
            "CLIENTE": ["Cliente A", "Cliente B"],
            "CHASIS": ["ABC123", "DEF456"],
            "DEPTO": ["NUEVOS UNIVERSIDAD", "SEMINUEVOS SAN ANGEL"],
            "CONDICIONES": ["Contado", "Credito"],
            "F. FACTURA": [datetime(2025, 11, 15), datetime(2025, 11, 16)],
            "FACTURA": ["F001", "F002"],
            "U. BRUTA": [18419.29, 24216.05],
            "% COMISION": [0.14, 0.18],
            "COMISION": [2578.70, 4358.89],
            "TOMA": [100.0, 200.0],
            "BANCOS\nFINANCIAMIENTOS": [50.0, 75.0],
            "EDEGAS": [25.0, 30.0],
            "VERIFICACIÓN": [10.0, 15.0],
            "ACCESORIOS": [20.0, 25.0],
            "GARANTIAS": [5.0, 10.0],
            "SEGUROS": [15.0, 20.0],
            "PLACAS": [8.0, 12.0],
            "BONOS/ OTROS": [50.0, 100.0],
            "COMISION INGRESO TOTAL": [2862.70, 4846.89],
            "DESCUENTOS": [0.0, 0.0],
            "ACUMULADO DE COMISIONES": [2862.70, 4846.89],
            "ACUMULADO": [2862.70, 4846.89],
            "DESCUENTOS 2": [0.0, 0.0],
            "FECHA DE PAGO": [datetime(2025, 11, 30), datetime(2025, 11, 30)],
            "SEMANA": [46, 46],
        })

    def _create_data_with_users(self, sales_df: pd.DataFrame, users_df: pd.DataFrame) -> pd.DataFrame:
        sales_df.attrs['users_mapping'] = users_df
        return sales_df


class TestCalculateCommission(TestGocarCommissionStrategy):

    def test_should_return_empty_when_data_is_empty(self, strategy_nuevos):
        result = strategy_nuevos.calculate_commission(pd.DataFrame())

        assert result.empty

    def test_should_return_empty_when_no_users_mapping(self, strategy_nuevos, sales_data):
        result = strategy_nuevos.calculate_commission(sales_data)

        assert result.empty

    def test_should_filter_by_nuevos_department(self, strategy_nuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert len(result) == 1
        assert result["Negocio"].iloc[0] == "NUEVOS UNIVERSIDAD"

    def test_should_filter_by_seminuevos_department(self, strategy_seminuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_seminuevos.calculate_commission(data)

        assert len(result) == 1
        assert result["Negocio"].iloc[0] == "SEMINUEVOS SAN ANGEL"

    def test_should_include_required_columns(self, strategy_nuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert "Fecha" in result.columns
        assert "Rep ID" in result.columns
        assert "ID Transaccion" in result.columns
        assert "Utilidad Bruta" in result.columns
        assert "Comision" in result.columns
        assert "Comision Base" in result.columns

    def test_should_assign_correct_rep_id(self, strategy_nuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert result["Rep ID"].iloc[0] == "170865"

    def test_should_calculate_utilidad_bruta(self, strategy_nuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert result["Utilidad Bruta"].iloc[0] == pytest.approx(18419.29, rel=0.01)

    def test_should_calculate_comision_using_excel_formulas(self, strategy_nuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        comision_base = 18419.29 * 0.14
        comision_ingreso_total = comision_base + 100.0 + 50.0 + 25.0 + 10.0 + 20.0 + 5.0 + 15.0 + 8.0 + 50.0
        comision_final = comision_ingreso_total - 0.0
        assert result["Comision"].iloc[0] == pytest.approx(comision_final, rel=0.01)

    def test_should_format_fecha_as_first_day_of_month(self, strategy_nuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert result["Fecha"].iloc[0] == "2025-11-01"

    def test_should_show_fecha_de_pago_as_actual_date(self, strategy_nuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert result["Fecha de Pago"].iloc[0] == "2025-11-30"

    def test_should_generate_transaction_id(self, strategy_nuevos, sales_data, users_mapping):
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert "NUEVOS_" in result["ID Transaccion"].iloc[0]
        assert "ABC123" in result["ID Transaccion"].iloc[0]


class TestFilterByRepId(TestGocarCommissionStrategy):

    def test_should_filter_by_rep_id(self, sales_data, users_mapping):
        strategy = GocarCommissionStrategy(
            department_type="NUEVOS",
            rep_id_filter="170865"
        )
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy.calculate_commission(data)

        assert len(result) == 1
        assert result["Rep ID"].iloc[0] == "170865"


class TestSkipInvalidRows(TestGocarCommissionStrategy):

    def test_should_skip_rows_without_fecha_pago(self, strategy_nuevos, users_mapping):
        sales_data = pd.DataFrame({
            "  AGENTE O VENDEDOR": ["KARLA RUIZ PALACIOS"],
            "CLIENTE": ["Cliente A"],
            "CHASIS": ["ABC123"],
            "DEPTO": ["NUEVOS UNIVERSIDAD"],
            "FECHA DE PAGO": [None],
            "ACUMULADO DE COMISIONES": [100.0],
        })
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert result.empty

    def test_should_skip_semana_header_rows(self, strategy_nuevos, users_mapping):
        sales_data = pd.DataFrame({
            "  AGENTE O VENDEDOR": ["SEMANA 26", "KARLA RUIZ PALACIOS"],
            "CLIENTE": [None, "Cliente A"],
            "CHASIS": [None, "ABC123"],
            "DEPTO": [None, "NUEVOS UNIVERSIDAD"],
            "FECHA DE PAGO": [None, datetime(2025, 11, 30)],
            "ACUMULADO DE COMISIONES": [None, 100.0],
        })
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert len(result) == 1
        assert result["Rep ID"].iloc[0] == "170865"

    def test_should_skip_rows_with_fecha_pago_before_2025(self, strategy_nuevos, users_mapping):
        sales_data = pd.DataFrame({
            "  AGENTE O VENDEDOR": ["KARLA RUIZ PALACIOS", "KARLA RUIZ PALACIOS", "KARLA RUIZ PALACIOS"],
            "CLIENTE": ["Cliente 1970", "Cliente 2022", "Cliente 2025"],
            "CHASIS": ["ABC111", "ABC222", "ABC333"],
            "DEPTO": ["NUEVOS UNIVERSIDAD", "NUEVOS UNIVERSIDAD", "NUEVOS UNIVERSIDAD"],
            "FECHA DE PAGO": [datetime(1970, 1, 1), datetime(2022, 6, 15), datetime(2025, 11, 30)],
            "ACUMULADO DE COMISIONES": [100.0, 200.0, 300.0],
        })
        data = self._create_data_with_users(sales_data, users_mapping)

        result = strategy_nuevos.calculate_commission(data)

        assert len(result) == 1
        assert result["Cliente"].iloc[0] == "Cliente 2025"
