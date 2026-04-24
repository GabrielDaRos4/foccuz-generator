import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.scania.service_manager import (
    ServiceManagerStrategy,
)
from tests.mothers.commissions.infrastructure.scania_dataframe_mother import (
    ScaniaDataFrameMother,
)


class TestServiceManagerStrategy:

    @pytest.fixture
    def strategy(self):
        return ServiceManagerStrategy(role_filter=["jefe de servicio"])

    @pytest.fixture
    def service_manager_data(self):
        return ScaniaDataFrameMother.service_manager_data()


class TestSalesPayment(TestServiceManagerStrategy):

    def test_should_pay_max_tier_when_120_percent_large(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe de servicio',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.20,
            'resultado_nps': 1.0,
            'tamano': 'grande'
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Cumplimiento Venta'].iloc[0] == 1000000

    def test_should_pay_nothing_when_below_100_percent(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe de servicio',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 0.95,
            'resultado_nps': 1.0,
            'tamano': 'grande'
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Cumplimiento Venta'].iloc[0] == 0

    def test_should_use_medium_thresholds_by_default(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe de servicio',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.20,
            'resultado_nps': 1.0
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Cumplimiento Venta'].iloc[0] == 950000


class TestNpsPayment(TestServiceManagerStrategy):

    def test_should_pay_max_when_nps_100_percent(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe de servicio',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.0,
            'resultado_nps': 1.0,
            'tamano': 'mediana'
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago NPS'].iloc[0] == 150000

    def test_should_pay_nothing_when_nps_below_80_percent(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe de servicio',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.0,
            'resultado_nps': 0.75,
            'tamano': 'mediana'
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago NPS'].iloc[0] == 0


class TestTeamManagement(TestServiceManagerStrategy):

    def test_should_calculate_turnover_compliance_tier_1(self, strategy):
        result = strategy._turnover_to_compliance(0.01)

        assert result == 1.10

    def test_should_calculate_turnover_compliance_tier_2(self, strategy):
        result = strategy._turnover_to_compliance(0.015)

        assert result == 1.00

    def test_should_calculate_absenteeism_compliance_tier_1(self, strategy):
        result = strategy._absenteeism_to_compliance(0.030)

        assert result == 1.10

    def test_should_calculate_absenteeism_compliance_tier_4(self, strategy):
        result = strategy._absenteeism_to_compliance(0.07)

        assert result == 0.80


class TestWipFactor(TestServiceManagerStrategy):

    def test_should_return_1_05_when_wip_below_20(self, strategy):
        result = strategy._wip_to_factor(0.19)

        assert result == 1.05

    def test_should_return_0_90_when_wip_above_30(self, strategy):
        result = strategy._wip_to_factor(0.35)

        assert result == 0.90


class TestEbitFactor(TestServiceManagerStrategy):

    def test_should_return_0_when_ebit_below_6(self, strategy):
        result = strategy._ebit_to_factor(0.05)

        assert result == 0.00

    def test_should_return_0_30_when_ebit_above_26(self, strategy):
        result = strategy._ebit_to_factor(0.30)

        assert result == 0.30


class TestFinalCalculation(TestServiceManagerStrategy):

    def test_should_calculate_final_payment_with_all_factors(
        self, strategy, service_manager_data
    ):
        result = strategy.calculate_commission(service_manager_data)

        assert 'Pago Final' in result.columns
        assert 'Comisión' in result.columns
        assert result['Comisión'].iloc[0] > 0

    def test_should_use_guaranteed_when_higher(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe de servicio',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 0.80,
            'resultado_nps': 0.70,
            'tamano': 'mediana',
            'garantizado': 500000
        }])

        result = strategy.calculate_commission(data)

        assert result['Comisión'].iloc[0] == 500000
