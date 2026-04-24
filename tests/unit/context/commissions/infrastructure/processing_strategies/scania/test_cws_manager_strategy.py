import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.scania.cws.cws_manager_strategy import (
    CWSManagerStrategy,
)
from tests.mothers.commissions.infrastructure.scania_dataframe_mother import (
    ScaniaDataFrameMother,
)


class TestCWSManagerStrategy:

    @pytest.fixture
    def strategy(self):
        return CWSManagerStrategy(role_filter=["jefe cws"])

    @pytest.fixture
    def cws_data(self):
        return ScaniaDataFrameMother.cws_manager_data()


class TestStockAdminPayment(TestCWSManagerStrategy):

    def test_should_pay_max_when_100_percent_compliance(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe cws',
            'branchid': 'SCL001',
            'days_worked': 30,
            '% ajuste de inventario': 0.005,
            'inventario rotativo pendiente': 0,
            'arribo fuera de plazo': 0,
            'real ubicacion de repuestos': 100,
            'meta ubicacion de repuestos': 100,
            'real disponibilidad de flota': 100,
            'meta disponibilidad de flota': 95
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Administración Stock'].iloc[0] == 200000

    def test_should_pay_100k_when_96_to_99_percent(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe cws',
            'branchid': 'SCL001',
            'days_worked': 30,
            '% ajuste de inventario': 0.01,
            'inventario rotativo pendiente': 0,
            'arribo fuera de plazo': 0,
            'real ubicacion de repuestos': 88,
            'meta ubicacion de repuestos': 100,
            'real disponibilidad de flota': 100,
            'meta disponibilidad de flota': 95
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Administración Stock'].iloc[0] == 100000


class TestFleetAvailability(TestCWSManagerStrategy):

    def test_should_pay_100k_when_meets_target(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe cws',
            'branchid': 'SCL001',
            'days_worked': 30,
            '% ajuste de inventario': 0.02,
            'inventario rotativo pendiente': 1,
            'arribo fuera de plazo': 1,
            'real ubicacion de repuestos': 80,
            'meta ubicacion de repuestos': 100,
            'real disponibilidad de flota': 98,
            'meta disponibilidad de flota': 95
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Disponibilidad Flota'].iloc[0] == 100000

    def test_should_pay_nothing_when_below_target(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe cws',
            'branchid': 'SCL001',
            'days_worked': 30,
            '% ajuste de inventario': 0.02,
            'inventario rotativo pendiente': 1,
            'arribo fuera de plazo': 1,
            'real ubicacion de repuestos': 80,
            'meta ubicacion de repuestos': 100,
            'real disponibilidad de flota': 90,
            'meta disponibilidad de flota': 95
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Disponibilidad Flota'].iloc[0] == 0


class TestOpenWorkOrders(TestCWSManagerStrategy):

    def test_should_use_custom_payment_when_provided(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe cws',
            'branchid': 'SCL001',
            'days_worked': 30,
            '% ajuste de inventario': 0.02,
            'inventario rotativo pendiente': 1,
            'arribo fuera de plazo': 1,
            'real ubicacion de repuestos': 80,
            'meta ubicacion de repuestos': 100,
            'real disponibilidad de flota': 90,
            'meta disponibilidad de flota': 95,
            'pago ot abiertas': 250000
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago OT Abiertas'].iloc[0] == 250000

    def test_should_use_default_150k_when_no_payment(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe cws',
            'branchid': 'SCL001',
            'days_worked': 30,
            '% ajuste de inventario': 0.02,
            'inventario rotativo pendiente': 1,
            'arribo fuera de plazo': 1,
            'real ubicacion de repuestos': 80,
            'meta ubicacion de repuestos': 100,
            'real disponibilidad de flota': 90,
            'meta disponibilidad de flota': 95
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago OT Abiertas'].iloc[0] == 150000


class TestFinalCalculation(TestCWSManagerStrategy):

    def test_should_sum_all_payments(self, strategy, cws_data):
        result = strategy.calculate_commission(cws_data)

        assert 'Pago Final' in result.columns
        assert 'Comisión' in result.columns
        assert result['Comisión'].iloc[0] > 0

    def test_should_prorate_by_days_worked(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe cws',
            'branchid': 'SCL001',
            'days_worked': 15,
            '% ajuste de inventario': 0.005,
            'inventario rotativo pendiente': 0,
            'arribo fuera de plazo': 0,
            'real ubicacion de repuestos': 100,
            'meta ubicacion de repuestos': 100,
            'real disponibilidad de flota': 100,
            'meta disponibilidad de flota': 95,
            'pago ot abiertas': 150000
        }])

        result = strategy.calculate_commission(data)

        expected = (200000 + 100000 + 150000) * 15 / 30
        assert result['Comisión'].iloc[0] == int(expected)
