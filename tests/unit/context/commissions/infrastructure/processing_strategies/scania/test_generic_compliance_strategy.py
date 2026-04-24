import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.scania.compliance import (
    GenericComplianceStrategy,
)


class TestGenericComplianceStrategy:

    @pytest.fixture
    def strategy(self):
        return GenericComplianceStrategy(
            role_filter=["operario bodega siniestros"]
        )


class TestDefaultThresholds(TestGenericComplianceStrategy):

    def test_should_pay_300k_when_above_110_percent(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento': 115
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Cumplimiento'].iloc[0] == 300000

    def test_should_pay_200k_when_100_to_110_percent(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento': 105
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Cumplimiento'].iloc[0] == 200000

    def test_should_pay_100k_when_90_to_100_percent(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento': 95
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Cumplimiento'].iloc[0] == 100000

    def test_should_exclude_from_output_when_below_90_percent(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento': 85
        }])

        result = strategy.calculate_commission(data)

        assert len(result) == 0


class TestCustomThresholds(TestGenericComplianceStrategy):

    def test_should_use_custom_thresholds(self):
        custom_thresholds = [
            (100, float("inf"), 500000),
            (80, 99.99, 250000),
            (0, 79.99, 0),
        ]
        strategy = GenericComplianceStrategy(
            role_filter=["test"],
            thresholds=custom_thresholds
        )
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'test',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento': 100
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Cumplimiento'].iloc[0] == 500000


class TestComplianceColumnDetection(TestGenericComplianceStrategy):

    def test_should_find_custom_compliance_column(self):
        strategy = GenericComplianceStrategy(
            role_filter=["test"],
            compliance_column="mi_cumplimiento"
        )
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'test',
            'branchid': 'SCL001',
            'days_worked': 30,
            'mi_cumplimiento': 115
        }])

        result = strategy.calculate_commission(data)

        assert result['Pago Cumplimiento'].iloc[0] == 300000

    def test_should_calculate_from_sales_and_target(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'venta_actual': 110000,
            'meta': 100000
        }])

        result = strategy.calculate_commission(data)

        assert result['Cumplimiento'].iloc[0] == 1.10
        assert result['Pago Cumplimiento'].iloc[0] == 300000


class TestComplianceNormalization(TestGenericComplianceStrategy):

    def test_should_normalize_percentage_values(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento': 110
        }])

        result = strategy.calculate_commission(data)

        assert result['Cumplimiento'].iloc[0] == 1.10

    def test_should_keep_decimal_values_as_is(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento': 1.10
        }])

        result = strategy.calculate_commission(data)

        assert result['Cumplimiento'].iloc[0] == 1.10
