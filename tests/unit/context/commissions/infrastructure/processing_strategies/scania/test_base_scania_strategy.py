import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.scania.compliance import (
    GenericComplianceStrategy,
)
from tests.mothers.commissions.infrastructure.scania_dataframe_mother import (
    ScaniaDataFrameMother,
)


class TestBaseScaniaStrategy:

    @pytest.fixture
    def strategy(self):
        return GenericComplianceStrategy(
            role_filter=["operario bodega siniestros"]
        )

    @pytest.fixture
    def employees_data(self):
        return ScaniaDataFrameMother.generic_compliance_data()


class TestRoleFiltering(TestBaseScaniaStrategy):

    def test_should_filter_employees_by_role(self, strategy, employees_data):
        result = strategy.calculate_commission(employees_data)

        assert len(result) == 2

    def test_should_return_empty_when_no_matching_role(self, employees_data):
        strategy = GenericComplianceStrategy(
            role_filter=["non_existent_role"]
        )

        result = strategy.calculate_commission(employees_data)

        assert result.empty

    def test_should_return_all_when_no_role_filter(self, employees_data):
        strategy = GenericComplianceStrategy(role_filter=None)

        result = strategy.calculate_commission(employees_data)

        assert len(result) == 2


class TestDaysProration(TestBaseScaniaStrategy):

    def test_should_prorate_commission_by_days_worked(self):
        strategy = GenericComplianceStrategy(
            role_filter=["operario bodega siniestros"]
        )
        data = pd.DataFrame([
            {
                'id empleado': 1001,
                'rut': '16.766.611-6',
                'cargo': 'operario bodega siniestros',
                'branchid': 'SCL001',
                'days_worked': 15,
                'cumplimiento': 110
            }
        ])

        result = strategy.calculate_commission(data)

        assert result['Comisión'].iloc[0] == 150000


class TestOutputFormatting(TestBaseScaniaStrategy):

    def test_should_include_required_output_columns(
        self, strategy, employees_data
    ):
        result = strategy.calculate_commission(employees_data)

        assert 'Fecha' in result.columns
        assert 'Rep ID' in result.columns
        assert 'ID Transacción' in result.columns
        assert 'Comisión' in result.columns

    def test_should_rename_id_empleado_to_rep_id(
        self, strategy, employees_data
    ):
        result = strategy.calculate_commission(employees_data)

        assert 'Rep ID' in result.columns


class TestEmptyInput(TestBaseScaniaStrategy):

    def test_should_return_empty_result_when_input_empty(self, strategy):
        result = strategy.calculate_commission(pd.DataFrame())

        assert result.empty
        assert 'Fecha' in result.columns
        assert 'Comisión' in result.columns


class TestCompliancePaymentCalculation(TestBaseScaniaStrategy):

    def test_should_calculate_compliance_payment_tier_1(self, strategy):
        result = strategy.calculate_compliance_payment(1.10, [
            (110, float("inf"), 300000),
            (100, 109.99, 200000),
            (90, 99.99, 100000),
            (0, 89.99, 0),
        ])

        assert result == 300000

    def test_should_calculate_compliance_payment_tier_2(self, strategy):
        result = strategy.calculate_compliance_payment(1.05, [
            (110, float("inf"), 300000),
            (100, 109.99, 200000),
            (90, 99.99, 100000),
            (0, 89.99, 0),
        ])

        assert result == 200000

    def test_should_return_zero_when_below_threshold(self, strategy):
        result = strategy.calculate_compliance_payment(0.80, [
            (110, float("inf"), 300000),
            (100, 109.99, 200000),
            (90, 99.99, 100000),
            (0, 89.99, 0),
        ])

        assert result == 0

    def test_should_return_zero_when_nan(self, strategy):
        result = strategy.calculate_compliance_payment(
            float('nan'),
            [(100, float("inf"), 100000)]
        )

        assert result == 0
