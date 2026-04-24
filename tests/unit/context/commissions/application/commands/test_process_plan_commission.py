from unittest.mock import MagicMock

import pandas as pd

from src.context.commissions.application.commands import (
    ProcessPlanCommissionCommand,
    ProcessPlanCommissionHandler,
)
from src.context.commissions.application.dto import PlanExecutionResult
from src.context.commissions.domain.aggregates import Plan
from src.context.commissions.domain.value_objects import DataSourceCollection, DataSourceConfig
from tests.mothers.commissions.domain.aggregates_mother import PlanMother, TenantMother
from tests.mothers.commissions.domain.value_objects_mother import (
    OutputConfigMother,
    StrategyConfigMother,
)


def _make_handler(
    data=None,
    strategy_result=None,
    export_error=None,
):
    data_repo = MagicMock()
    data_repo.get_data_for_plan.return_value = data if data is not None else pd.DataFrame()

    calculator = MagicMock()
    if strategy_result is not None:
        calculator.calculate.return_value = strategy_result
    else:
        calculator.calculate.return_value = pd.DataFrame()

    exporter = MagicMock()
    if export_error:
        exporter.export.side_effect = export_error

    strategy_factory = MagicMock()
    strategy_factory.create_strategy.return_value = MagicMock()

    handler = ProcessPlanCommissionHandler(data_repo, calculator, exporter, strategy_factory)
    return handler, data_repo, calculator, exporter, strategy_factory


def _make_command(tenant=None, plan=None, target_period=None, dependency_results=None):
    return ProcessPlanCommissionCommand(
        tenant=tenant or TenantMother.active(),
        plan=plan or PlanMother.active(),
        target_period=target_period,
        dependency_results=dependency_results,
    )


class TestProcessPlanCommissionHandler:

    def test_handle_delegates_to_handle_with_data(self):
        data = pd.DataFrame({"name": ["Alice"], "comision_total": [1000.0]})
        result_data = data.copy()
        handler, _, calculator, _, _ = _make_handler(data=data, strategy_result=result_data)

        result = handler.handle(_make_command())

        assert isinstance(result, PlanExecutionResult)
        assert result.success is True

    def test_should_return_success_with_commission(self):
        data = pd.DataFrame({"name": ["Alice", "Bob"], "amount": [100, 200]})
        result_data = pd.DataFrame({"name": ["Alice", "Bob"], "comision_total": [500.0, 700.0]})
        handler, _, _, exporter, _ = _make_handler(data=data, strategy_result=result_data)

        result, df = handler.handle_with_data(_make_command())

        assert result.success is True
        assert result.records_processed == 2
        assert result.total_commission == 1200.0
        exporter.export.assert_called_once()

    def test_should_return_empty_when_no_data(self):
        handler, _, _, exporter, _ = _make_handler(data=pd.DataFrame())

        result, df = handler.handle_with_data(_make_command())

        assert result.success is True
        assert result.records_processed == 0
        assert result.total_commission == 0.0
        assert df.empty
        exporter.export.assert_not_called()

    def test_should_return_empty_when_result_empty(self):
        data = pd.DataFrame({"name": ["Alice"]})
        handler, _, _, exporter, _ = _make_handler(data=data, strategy_result=pd.DataFrame())

        result, df = handler.handle_with_data(_make_command())

        assert result.success is True
        assert result.records_processed == 0
        assert df.empty
        exporter.export.assert_not_called()

    def test_should_handle_exception_gracefully(self):
        data = pd.DataFrame({"name": ["Alice"]})
        handler, data_repo, _, _, _ = _make_handler(data=data)
        data_repo.get_data_for_plan.side_effect = RuntimeError("DB connection failed")

        result, df = handler.handle_with_data(_make_command())

        assert result.success is False
        assert "DB connection failed" in result.error_message
        assert df.empty

    def test_should_find_comision_total_column(self):
        data = pd.DataFrame({"name": ["Alice"], "comision_total": [500.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)

        result, _ = handler.handle_with_data(_make_command())

        assert result.total_commission == 500.0

    def test_should_find_commission_column(self):
        data = pd.DataFrame({"name": ["Alice"], "commission": [300.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)

        result, _ = handler.handle_with_data(_make_command())

        assert result.total_commission == 300.0

    def test_should_return_zero_when_no_commission_column(self):
        data = pd.DataFrame({"name": ["Alice"], "amount": [1000.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)

        result, _ = handler.handle_with_data(_make_command())

        assert result.total_commission == 0.0

    def test_should_normalize_period_yyyy_mm(self):
        data = pd.DataFrame({"name": ["Alice"], "comision": [100.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)

        handler.handle_with_data(_make_command(target_period="2026-04"))

        PlanMother.active()
        assert ProcessPlanCommissionHandler._normalize_period("2026-04") == "2026-04-01"

    def test_should_normalize_period_full_date(self):
        assert ProcessPlanCommissionHandler._normalize_period("2026-04-15") == "2026-04-15"

    def test_should_normalize_period_none(self):
        assert ProcessPlanCommissionHandler._normalize_period(None) is None

    def test_should_inject_period_into_s3_sources(self):
        s3_source = DataSourceConfig(
            source_id="s3_source",
            source_type="s3",
            config={"pattern": "data/sales_*.csv", "bucket": "test"}
        )
        plan = Plan(
            id="P1", name="Test", tenant_id="T1", active=True,
            data_sources=DataSourceCollection(sources=[s3_source]),
            output_config=OutputConfigMother.default(),
            strategy_config=StrategyConfigMother.tiered_commission(),
        )

        ProcessPlanCommissionHandler._inject_period_into_s3_sources(plan, "2026-04-01")

        assert s3_source.config["pattern"] == "data/sales_202604.csv"

    def test_should_not_inject_period_when_no_wildcard(self):
        s3_source = DataSourceConfig(
            source_id="s3_source",
            source_type="s3",
            config={"pattern": "data/fixed.csv", "bucket": "test"}
        )
        plan = Plan(
            id="P1", name="Test", tenant_id="T1", active=True,
            data_sources=DataSourceCollection(sources=[s3_source]),
            output_config=OutputConfigMother.default(),
            strategy_config=StrategyConfigMother.tiered_commission(),
        )

        ProcessPlanCommissionHandler._inject_period_into_s3_sources(plan, "2026-04-01")

        assert s3_source.config["pattern"] == "data/fixed.csv"

    def test_should_skip_non_s3_sources_for_period_injection(self):
        plan = PlanMother.active()

        ProcessPlanCommissionHandler._inject_period_into_s3_sources(plan, "2026-04-01")

    def test_should_use_dependency_results_when_available(self):
        dep_data = pd.DataFrame({"dep_col": [1, 2, 3]})
        plan = PlanMother.active(plan_id="MAIN")
        plan.depends_on = ["DEP"]

        main_data = pd.DataFrame({"name": ["Alice"], "comision_total": [100.0]})
        main_data.attrs['sources'] = {}

        handler, data_repo, _, _, _ = _make_handler(
            data=main_data, strategy_result=main_data
        )

        result, _ = handler.handle_with_data(_make_command(
            plan=plan,
            dependency_results={"DEP": dep_data}
        ))

        assert result.success is True

    def test_should_call_data_repo_when_no_dependencies(self):
        data = pd.DataFrame({"name": ["Alice"], "comision_total": [100.0]})
        handler, data_repo, _, _, _ = _make_handler(data=data, strategy_result=data)
        plan = PlanMother.active()
        plan.depends_on = []

        handler.handle_with_data(_make_command(plan=plan))

        data_repo.get_data_for_plan.assert_called_once()

    def test_should_pass_target_period_to_strategy_config(self):
        data = pd.DataFrame({"name": ["Alice"], "comision_total": [100.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)
        plan = PlanMother.active()

        handler.handle_with_data(_make_command(plan=plan, target_period="2026-04"))

        assert plan.strategy_config.params["target_period"] == "2026-04-01"

    def test_should_use_current_period_when_no_target(self):
        data = pd.DataFrame({"name": ["Alice"], "comision_total": [100.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)
        plan = PlanMother.active()

        handler.handle_with_data(_make_command(plan=plan, target_period=None))

        assert "target_period" in plan.strategy_config.params

    def test_should_cache_result_data_in_plan_result(self):
        data = pd.DataFrame({"name": ["Alice"], "comision_total": [100.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)

        _, result_data = handler.handle_with_data(_make_command())

        assert not result_data.empty
        assert len(result_data) == 1

    def test_should_export_with_output_config(self):
        data = pd.DataFrame({"name": ["Alice"], "comision_total": [100.0]})
        handler, _, _, exporter, _ = _make_handler(data=data, strategy_result=data)
        plan = PlanMother.active()

        handler.handle_with_data(_make_command(plan=plan))

        call_kwargs = exporter.export.call_args
        assert call_kwargs[1]["output_config"] == plan.output_config
        assert call_kwargs[1]["plan_name"] == plan.name

    def test_should_detect_comision_column_name(self):
        data = pd.DataFrame({"comision": [500.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)

        result, _ = handler.handle_with_data(_make_command())

        assert result.total_commission == 500.0

    def test_should_detect_comision_capitalized_column_name(self):
        data = pd.DataFrame({"Comision": [500.0]})
        handler, _, _, _, _ = _make_handler(data=data, strategy_result=data)

        result, _ = handler.handle_with_data(_make_command())

        assert result.total_commission == 500.0


class TestBuildEmptyDataWarning:

    def test_should_report_empty_source(self):
        diagnostics = {
            "sources": {"ventas": {"rows": 0}},
        }

        result = ProcessPlanCommissionHandler._build_empty_data_warning(diagnostics)

        assert "Source 'ventas' returned 0 rows" in result

    def test_should_report_source_error(self):
        diagnostics = {
            "sources": {"ventas": {"rows": 5, "error": "Connection refused"}},
        }

        result = ProcessPlanCommissionHandler._build_empty_data_warning(diagnostics)

        assert "Connection refused" in result

    def test_should_report_merge_mismatch(self):
        diagnostics = {
            "sources": {},
            "merge": {
                "pre_merge_rows": 10,
                "post_merge_rows": 0,
                "employees_rows": 5,
                "plan_data_rows": 10,
            },
        }

        result = ProcessPlanCommissionHandler._build_empty_data_warning(diagnostics)

        assert "Merge produced no matches" in result

    def test_should_return_default_message_when_no_diagnostics(self):
        result = ProcessPlanCommissionHandler._build_empty_data_warning({})

        assert "No data available" in result


class TestBuildEmptyResultWarning:

    def _make_handler_instance(self):
        handler, _, _, _, _ = _make_handler()
        return handler

    def test_should_delegate_to_empty_data_warning_when_input_zero(self):
        handler = self._make_handler_instance()
        plan = PlanMother.active()

        result = handler._build_empty_result_warning({}, {}, plan, input_rows=0)

        assert "No data available" in result

    def test_should_report_plan_data_empty(self):
        handler = self._make_handler_instance()
        source_diagnostics = {"merge": {"plan_data_empty": True}}
        plan = PlanMother.active()

        result = handler._build_empty_result_warning(source_diagnostics, {}, plan, input_rows=5)

        assert "Plan data file is empty" in result

    def test_should_report_plan_data_source_error(self):
        handler = self._make_handler_instance()
        source_diagnostics = {
            "sources": {"plan_data": {"rows": 0, "error": "File not found"}},
        }
        plan = PlanMother.active()

        result = handler._build_empty_result_warning(source_diagnostics, {}, plan, input_rows=5)

        assert "File not found" in result

    def test_should_report_role_filter_mismatch(self):
        handler = self._make_handler_instance()
        plan = PlanMother.active()
        plan.strategy_config.params["role_filter"] = ["Manager"]
        source_diagnostics = {
            "sources": {"plan_data": {"rows": 10}},
        }
        result_diagnostics = {
            "matched_rows": 0,
            "filtered_out_by_role": 10,
            "available_roles": ["Engineer", "Director"],
        }

        result = handler._build_empty_result_warning(
            source_diagnostics, result_diagnostics, plan, input_rows=10
        )

        assert "Role filter" in result
        assert "Engineer" in result

    def test_should_report_zero_commission_filter(self):
        handler = self._make_handler_instance()
        plan = PlanMother.active()
        source_diagnostics = {
            "sources": {"plan_data": {"rows": 10}},
        }
        result_diagnostics = {"matched_rows": 5}

        result = handler._build_empty_result_warning(
            source_diagnostics, result_diagnostics, plan, input_rows=10
        )

        assert "$0 commission" in result

    def test_should_return_default_when_no_diagnostics(self):
        handler = self._make_handler_instance()
        plan = PlanMother.active()
        source_diagnostics = {
            "sources": {"plan_data": {"rows": 10}},
        }

        result = handler._build_empty_result_warning(
            source_diagnostics, {}, plan, input_rows=10
        )

        assert "Processing returned 0 records" in result

    def test_should_report_plan_data_source_empty_no_error(self):
        handler = self._make_handler_instance()
        source_diagnostics = {
            "sources": {"plan_data": {"rows": 0}},
        }
        plan = PlanMother.active()

        result = handler._build_empty_result_warning(source_diagnostics, {}, plan, input_rows=5)

        assert "Plan data source is empty" in result

    def test_should_report_role_filter_no_available_roles(self):
        handler = self._make_handler_instance()
        plan = PlanMother.active()
        plan.strategy_config.params["role_filter"] = ["Manager"]
        source_diagnostics = {
            "sources": {"plan_data": {"rows": 10}},
        }
        result_diagnostics = {
            "matched_rows": 0,
            "filtered_out_by_role": 10,
        }

        result = handler._build_empty_result_warning(
            source_diagnostics, result_diagnostics, plan, input_rows=10
        )

        assert "Role filter" in result
        assert "Available roles" not in result
