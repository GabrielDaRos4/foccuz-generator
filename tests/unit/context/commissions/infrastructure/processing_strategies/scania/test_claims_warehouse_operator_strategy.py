import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.scania.claims import (
    ClaimsWarehouseOperatorStrategy,
    OperarioBodegaSiniestrosStrategy,
)


class TestClaimsWarehouseOperatorStrategy:

    @pytest.fixture
    def strategy(self):
        return ClaimsWarehouseOperatorStrategy(
            role_filter=["operario bodega siniestros"]
        )


class TestBackwardCompatibility(TestClaimsWarehouseOperatorStrategy):

    def test_should_have_spanish_alias(self):
        assert OperarioBodegaSiniestrosStrategy is ClaimsWarehouseOperatorStrategy

    def test_should_work_with_spanish_alias(self):
        strategy = OperarioBodegaSiniestrosStrategy(
            role_filter=["operario bodega siniestros"]
        )
        assert isinstance(strategy, ClaimsWarehouseOperatorStrategy)


class TestComplianceThresholds(TestClaimsWarehouseOperatorStrategy):

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

        assert result['Comisión'].iloc[0] == 300000

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

        assert result['Comisión'].iloc[0] == 200000

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

        assert result['Comisión'].iloc[0] == 100000

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


class TestSalesDataExtraction(TestClaimsWarehouseOperatorStrategy):

    def test_should_extract_sales_from_venta_column(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'venta real': 100000,
            'meta': 100000
        }])

        result = strategy.calculate_commission(data)

        assert result['Venta'].iloc[0] == 100000

    def test_should_extract_target_from_meta_column(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'actual': 120000,
            'budget': 100000
        }])

        result = strategy.calculate_commission(data)

        assert result['Meta'].iloc[0] == 100000


class TestComplianceCalculation(TestClaimsWarehouseOperatorStrategy):

    def test_should_calculate_compliance_from_sales_and_target(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'venta': 110000,
            'meta': 100000
        }])

        result = strategy.calculate_commission(data)

        assert result['Cumplimiento Venta'].iloc[0] == 1.1

    def test_should_normalize_percentage_compliance(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento': 110
        }])

        result = strategy.calculate_commission(data)

        assert result['Cumplimiento Venta'].iloc[0] == 1.10


class TestDaysProration(TestClaimsWarehouseOperatorStrategy):

    def test_should_prorate_by_days_worked(self, strategy):
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'operario bodega siniestros',
            'branchid': 'SCL001',
            'days_worked': 15,
            'cumplimiento': 110
        }])

        result = strategy.calculate_commission(data)

        assert result['Comisión'].iloc[0] == 150000
