import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.merge.branch_merger import (
    enrich_with_branch_data,
    try_merge_by_branch,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.merge.buk_enricher import (
    enrich_with_payroll_details,
    flatten_buk_nested_fields,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.merge.rut_merger import (
    try_merge_by_rut,
)

logger = logging.getLogger(__name__)


def scania_generic_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict
) -> pd.DataFrame:
    employees_key = config.get("employees_key", "employees")
    plan_data_key = config.get("plan_data_key", "plan_data")

    diagnostics = _initialize_diagnostics()

    employees_df = dataframes.get(employees_key)
    plan_data_df = dataframes.get(plan_data_key)

    _update_source_diagnostics(diagnostics, employees_key, employees_df)
    _update_source_diagnostics(diagnostics, plan_data_key, plan_data_df)

    employees_df = _validate_and_prepare_employees(employees_df, employees_key, diagnostics)
    employees_df = _enrich_employees(employees_df, config)

    diagnostics["merge"]["employees_rows"] = len(employees_df)

    if plan_data_df is None or plan_data_df.empty:
        logger.warning(f"Plan data '{plan_data_key}' is empty, returning employees only")
        diagnostics["merge"]["plan_data_empty"] = True
        employees_df.attrs["diagnostics"] = diagnostics
        return employees_df

    plan_data_df = _prepare_plan_data(plan_data_df)
    _update_merge_diagnostics(diagnostics, len(plan_data_df), len(employees_df))

    logger.info(
        f"Merging: employees ({len(employees_df)} rows) + "
        f"plan_data ({len(plan_data_df)} rows)"
    )

    result = _perform_merge(employees_df, plan_data_df, diagnostics)
    result.attrs["diagnostics"] = diagnostics
    return result


def _initialize_diagnostics() -> dict:
    return {
        "sources": {},
        "merge": {}
    }


def _update_source_diagnostics(
    diagnostics: dict,
    key: str,
    df: pd.DataFrame | None
) -> None:
    diagnostics["sources"][key] = {
        "rows": len(df) if df is not None else 0,
        "error": None if df is not None and not df.empty else "Empty or not found"
    }


def _update_merge_diagnostics(
    diagnostics: dict,
    plan_data_rows: int,
    employees_rows: int
) -> None:
    diagnostics["merge"]["plan_data_rows"] = plan_data_rows
    diagnostics["merge"]["pre_merge_rows"] = employees_rows


def _validate_and_prepare_employees(
    employees_df: pd.DataFrame | None,
    employees_key: str,
    diagnostics: dict
) -> pd.DataFrame:
    if employees_df is None or employees_df.empty:
        empty_result = pd.DataFrame()
        empty_result.attrs["diagnostics"] = diagnostics
        raise ValueError(
            f"Employee data '{employees_key}' is required but not found or empty"
        )

    employees_df = employees_df.copy()
    employees_df.columns = employees_df.columns.str.lower().str.strip()
    return employees_df


def _enrich_employees(employees_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    target_period = config.get("target_period")
    employees_df = flatten_buk_nested_fields(employees_df, target_period)
    employees_df = enrich_with_payroll_details(employees_df, config)
    return employees_df


def _prepare_plan_data(plan_data_df: pd.DataFrame) -> pd.DataFrame:
    plan_data_df = plan_data_df.copy()
    plan_data_df.columns = plan_data_df.columns.str.lower().str.strip()
    return plan_data_df


def _perform_merge(
    employees_df: pd.DataFrame,
    plan_data_df: pd.DataFrame,
    diagnostics: dict
) -> pd.DataFrame:
    merge_result = try_merge_by_rut(employees_df, plan_data_df)
    if merge_result is not None:
        merge_result = enrich_with_branch_data(merge_result, plan_data_df)
        merge_result = _enrich_with_secondary_arrays(merge_result, plan_data_df)
        diagnostics["merge"]["merge_type"] = "rut"
        diagnostics["merge"]["post_merge_rows"] = len(merge_result)
        return merge_result

    merge_result = try_merge_by_branch(employees_df, plan_data_df)
    if merge_result is not None:
        merge_result = _enrich_with_secondary_arrays(merge_result, plan_data_df)
        diagnostics["merge"]["merge_type"] = "branch"
        diagnostics["merge"]["post_merge_rows"] = len(merge_result)
        return merge_result

    logger.warning("No common merge key found, concatenating data")
    diagnostics["merge"]["merge_type"] = "concat"
    result = pd.concat([employees_df, plan_data_df], axis=1)
    diagnostics["merge"]["post_merge_rows"] = len(result)
    return result


def _enrich_with_secondary_arrays(
    result: pd.DataFrame,
    plan_data_df: pd.DataFrame
) -> pd.DataFrame:
    secondary_arrays = plan_data_df.attrs.get('secondary_arrays', {})
    if not secondary_arrays:
        return result

    detail_arrays = {}
    for key, secondary_df in secondary_arrays.items():
        is_detail = any(
            pattern in key.lower()
            for pattern in [
                "contrato", "visita", "venta", "leadtime", "productividad",
                "eficiencia", "estudio", "configuracion", "retorno", "margenes", "nps",
            ]
        )
        if is_detail:
            detail_arrays[key] = secondary_df.copy()
        else:
            result = _merge_secondary_by_branch(result, secondary_df, key)

    if detail_arrays:
        result.attrs['secondary_arrays'] = detail_arrays
        logger.info(f"Preserved {len(detail_arrays)} detail arrays for strategy: {list(detail_arrays.keys())}")

    return result


def _merge_secondary_by_branch(
    result: pd.DataFrame,
    secondary_df: pd.DataFrame,
    key: str
) -> pd.DataFrame:
    from src.context.commissions.infrastructure.processing_strategies.custom.scania.merge.normalizers import (
        normalize_branch,
    )

    emp_branch_col = next(
        (c for c in result.columns if c.lower() == "branch"), None
    )
    sec_branch_col = next(
        (c for c in secondary_df.columns if c.lower() == "branch"), None
    )

    if len(secondary_df) == 1 and not sec_branch_col:
        result = result.copy()
        for col in secondary_df.columns:
            col_lower = col.lower()
            if col_lower not in [c.lower() for c in result.columns]:
                result[col] = secondary_df[col].iloc[0]
        logger.info(f"Broadcast '{key}' single row to all {len(result)} employees")
        return result

    if not emp_branch_col or not sec_branch_col:
        logger.debug(f"No branch column found: emp={emp_branch_col}, sec={sec_branch_col}")
        return result

    result = result.copy()
    secondary_df = secondary_df.copy()

    result["_branch_merge"] = normalize_branch(result[emp_branch_col])
    secondary_df["_branch_merge"] = normalize_branch(secondary_df[sec_branch_col])

    emp_branches = set(result["_branch_merge"].dropna().unique())
    sec_branches = set(secondary_df["_branch_merge"].dropna().unique())
    matching = emp_branches.intersection(sec_branches)
    logger.debug(f"Branch matching: emp has {len(emp_branches)}, sec has {len(sec_branches)}, matching {len(matching)}")
    if not matching and emp_branches and sec_branches:
        logger.debug(f"Emp samples: {list(emp_branches)[:5]}")
        logger.debug(f"Sec samples: {list(sec_branches)[:5]}")

    new_cols = [
        c for c in secondary_df.columns
        if c.lower() not in [col.lower() for col in result.columns]
        and c != "_branch_merge"
    ]

    if not new_cols:
        result = result.drop(columns=["_branch_merge"], errors="ignore")
        return result

    merge_cols = ["_branch_merge"] + new_cols
    secondary_subset = secondary_df[merge_cols].drop_duplicates(subset=["_branch_merge"])

    result = result.merge(secondary_subset, on="_branch_merge", how="left")
    result = result.drop(columns=["_branch_merge"], errors="ignore")

    logger.info(f"Enriched with secondary array '{key}' by branch: added {len(new_cols)} columns")
    return result
