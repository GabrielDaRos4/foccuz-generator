import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.scania.scania_merge import (
    _flatten_buk_nested_fields,
    scania_generic_merge,
)
from tests.mothers.commissions.infrastructure.scania_dataframe_mother import (
    ScaniaDataFrameMother,
)


class TestScaniaGenericMerge:

    @pytest.fixture
    def employees_df(self):
        return ScaniaDataFrameMother.employees()

    @pytest.fixture
    def plan_data_with_rut(self):
        return ScaniaDataFrameMother.plan_data_with_rut()

    @pytest.fixture
    def plan_data_with_branch(self):
        return ScaniaDataFrameMother.plan_data_with_branch()

    @pytest.fixture
    def default_config(self):
        return {
            "employees_key": "employees",
            "plan_data_key": "plan_data"
        }


class TestMergeByRut(TestScaniaGenericMerge):

    def test_should_merge_by_rut_when_both_have_rut_column(
        self, employees_df, plan_data_with_rut, default_config
    ):
        dataframes = {
            "employees": employees_df,
            "plan_data": plan_data_with_rut
        }

        result = scania_generic_merge(dataframes, default_config)

        assert len(result) == 3
        assert "cumplimiento venta" in result.columns

    def test_should_normalize_rut_formats_when_merging(
        self, default_config
    ):
        employees = pd.DataFrame([
            {'id empleado': 1, 'rut': '16.766.611-6', 'cargo': 'Test'}
        ])
        plan_data = pd.DataFrame([
            {'rut': '16766611-6', 'valor': 100}
        ])
        dataframes = {"employees": employees, "plan_data": plan_data}

        result = scania_generic_merge(dataframes, default_config)

        assert len(result) == 1
        assert result['valor'].iloc[0] == 100


class TestMergeByBranch(TestScaniaGenericMerge):

    def test_should_merge_by_branch_when_no_rut_in_plan_data(
        self, employees_df, plan_data_with_branch, default_config
    ):
        plan_data_no_rut = plan_data_with_branch.copy()
        dataframes = {
            "employees": employees_df,
            "plan_data": plan_data_no_rut
        }

        result = scania_generic_merge(dataframes, default_config)

        assert len(result) == 3
        assert "cumplimiento" in result.columns

    def test_should_merge_by_composite_key_branchid_and_branch(
        self, default_config
    ):
        employees = pd.DataFrame([
            {'id empleado': 1, 'branchid': '520', 'branch': 'Valparaiso', 'cargo': 'Test'},
            {'id empleado': 2, 'branchid': '500', 'branch': 'Santiago', 'cargo': 'Test'},
            {'id empleado': 3, 'branchid': '150', 'branch': 'Iquique', 'cargo': 'Test'},
        ])
        plan_data = pd.DataFrame([
            {'BRANCHID': '520', 'Branch': 'Valparaiso', 'cumplimiento': 95.0},
            {'BRANCHID': '500', 'Branch': 'Santiago', 'cumplimiento': 88.0},
            {'BRANCHID': '150', 'Branch': 'Iquique', 'cumplimiento': 102.0},
        ])
        dataframes = {"employees": employees, "plan_data": plan_data}

        result = scania_generic_merge(dataframes, default_config)

        assert len(result) == 3
        assert result[result['id empleado'] == 1]['cumplimiento'].iloc[0] == 95.0
        assert result[result['id empleado'] == 2]['cumplimiento'].iloc[0] == 88.0
        assert result[result['id empleado'] == 3]['cumplimiento'].iloc[0] == 102.0

    def test_should_fallback_to_branchid_when_composite_key_fails(
        self, default_config
    ):
        employees = pd.DataFrame([
            {'id empleado': 1, 'branchid': '520', 'branch': 'Santiago', 'cargo': 'Test'},
        ])
        plan_data = pd.DataFrame([
            {'BRANCHID': '520', 'Branch': 'Valparaiso', 'cumplimiento': 95.0},
        ])
        dataframes = {"employees": employees, "plan_data": plan_data}

        result = scania_generic_merge(dataframes, default_config)

        assert len(result) == 1
        assert result['cumplimiento'].iloc[0] == 95.0


class TestEdgeCases(TestScaniaGenericMerge):

    def test_should_raise_error_when_employees_empty(
        self, plan_data_with_rut, default_config
    ):
        dataframes = {
            "employees": pd.DataFrame(),
            "plan_data": plan_data_with_rut
        }

        with pytest.raises(ValueError) as exc_info:
            scania_generic_merge(dataframes, default_config)

        assert "Employee data" in str(exc_info.value)

    def test_should_return_employees_only_when_plan_data_empty(
        self, employees_df, default_config
    ):
        dataframes = {
            "employees": employees_df,
            "plan_data": pd.DataFrame()
        }

        result = scania_generic_merge(dataframes, default_config)

        assert len(result) == 3
        assert "id empleado" in result.columns

    def test_should_use_custom_keys_when_configured(self):
        employees = pd.DataFrame([
            {'id': 1, 'rut': '16.766.611-6'}
        ])
        plan = pd.DataFrame([
            {'rut': '16766611-6', 'valor': 100}
        ])
        config = {
            "employees_key": "custom_emp",
            "plan_data_key": "custom_plan"
        }
        dataframes = {
            "custom_emp": employees,
            "custom_plan": plan
        }

        result = scania_generic_merge(dataframes, config)

        assert len(result) == 1


class TestColumnNormalization(TestScaniaGenericMerge):

    def test_should_normalize_column_names_to_lowercase(
        self, default_config
    ):
        employees = pd.DataFrame([
            {'ID Empleado': 1, 'RUT': '16.766.611-6', 'CARGO': 'Test'}
        ])
        plan_data = pd.DataFrame([
            {'RUT': '16766611-6', 'VALOR': 100}
        ])
        dataframes = {"employees": employees, "plan_data": plan_data}

        result = scania_generic_merge(dataframes, default_config)

        assert 'id empleado' in result.columns
        assert 'cargo' in result.columns


class TestFlattenBukNestedFields:

    def test_should_extract_id_empleado_from_id(self):
        df = pd.DataFrame([{
            'id': 12345,
            'rut': '16.766.611-6',
            'current_job': {
                'role': {'name': 'Tecnico', 'id': 123},
                'custom_attributes': {
                    'Usuario': 'SCLFES'
                }
            }
        }])

        result = _flatten_buk_nested_fields(df)

        assert 'id empleado' in result.columns
        assert result['id empleado'].iloc[0] == 12345

    def test_should_fallback_to_id_when_no_usuario(self):
        df = pd.DataFrame([{
            'id': 12345,
            'rut': '16.766.611-6',
            'current_job': {
                'role': {'name': 'Tecnico', 'id': 123}
            }
        }])

        result = _flatten_buk_nested_fields(df)

        assert 'id empleado' in result.columns
        assert result['id empleado'].iloc[0] == 12345

    def test_should_extract_cargo_from_nested_current_job(self):
        df = pd.DataFrame([{
            'id': 1,
            'rut': '16.766.611-6',
            'current_job': {
                'role': {'name': 'Jefe de Taller', 'id': 123, 'code': 'jefe_de_taller'},
                'custom_attributes': {
                    'ubica': 'DIS',
                    'd_convenio': 'Santiago',
                    'd_ubica': 'Distribuidor'
                }
            }
        }])

        result = _flatten_buk_nested_fields(df)

        assert 'cargo' in result.columns
        assert result['cargo'].iloc[0] == 'jefe_de_taller'

    def test_should_extract_branch_from_custom_attributes(self):
        df = pd.DataFrame([{
            'id': 1,
            'rut': '16.766.611-6',
            'current_job': {
                'role': {'name': 'Tecnico', 'id': 123},
                'custom_attributes': {
                    'ubica': 'DIS',
                    'd_convenio': 'Concepcion',
                    'd_ubica': 'Distribuidor'
                }
            }
        }])

        result = _flatten_buk_nested_fields(df)

        assert 'branchid' in result.columns
        assert result['branchid'].iloc[0] == 'DIS'
        assert 'branch' in result.columns
        assert result['branch'].iloc[0] == 'Concepcion'

    def test_should_not_overwrite_existing_cargo_column(self):
        df = pd.DataFrame([{
            'id': 1,
            'cargo': 'Existing Role',
            'current_job': {
                'role': {'name': 'Different Role', 'id': 123}
            }
        }])

        result = _flatten_buk_nested_fields(df)

        assert result['cargo'].iloc[0] == 'Existing Role'

    def test_should_handle_missing_nested_fields(self):
        df = pd.DataFrame([{
            'id': 1,
            'rut': '16.766.611-6',
            'current_job': {'other_field': 'value'}
        }])

        result = _flatten_buk_nested_fields(df)

        assert 'cargo' in result.columns
        assert result['cargo'].iloc[0] is None

    def test_should_handle_missing_current_job_column(self):
        df = pd.DataFrame([{
            'id': 1,
            'rut': '16.766.611-6',
            'name': 'Employee'
        }])

        result = _flatten_buk_nested_fields(df)

        assert 'cargo' not in result.columns or result['cargo'].iloc[0] is None
