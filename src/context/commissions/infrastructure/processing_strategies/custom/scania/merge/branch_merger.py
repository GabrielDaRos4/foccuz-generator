import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.merge.normalizers import (
    normalize_branch,
)

logger = logging.getLogger(__name__)


def try_merge_by_branch(
    employees_df: pd.DataFrame,
    plan_data_df: pd.DataFrame
) -> pd.DataFrame | None:
    emp_branch_id = _find_branch_id_column(employees_df)
    emp_branch_name = _find_branch_name_column(employees_df)
    plan_branch_id = _find_branch_id_column(plan_data_df)
    plan_branch_name = _find_branch_name_column(plan_data_df)

    has_both_emp = emp_branch_id and emp_branch_name
    has_both_plan = plan_branch_id and plan_branch_name

    if has_both_emp and has_both_plan:
        return _merge_by_composite_key(
            employees_df, plan_data_df,
            emp_branch_id, emp_branch_name,
            plan_branch_id, plan_branch_name
        )

    if not plan_branch_id and not plan_branch_name:
        return None

    if not emp_branch_id and not emp_branch_name:
        return None

    return _merge_by_single_key(
        employees_df, plan_data_df,
        emp_branch_id, emp_branch_name,
        plan_branch_id, plan_branch_name
    )


def enrich_with_branch_data(
    merged_df: pd.DataFrame,
    plan_data_df: pd.DataFrame
) -> pd.DataFrame:
    emp_branch_id = _find_branch_id_column(merged_df)
    emp_branch_name = _find_branch_name_column(merged_df)
    plan_branch_col = _find_plan_branch_column(plan_data_df)

    if not plan_branch_col:
        return merged_df

    emp_branch_col = _select_employee_branch_column(
        emp_branch_id, emp_branch_name, plan_branch_col
    )

    if not emp_branch_col:
        return merged_df

    null_cols = _find_null_columns_to_fill(merged_df, plan_data_df)
    if not null_cols:
        return merged_df

    return _perform_branch_enrichment(
        merged_df, plan_data_df, emp_branch_col, plan_branch_col, null_cols
    )


def _find_branch_id_column(df: pd.DataFrame) -> str | None:
    return next(
        (c for c in df.columns if "branch" in c.lower() and "id" in c.lower()),
        None
    )


def _find_branch_name_column(df: pd.DataFrame) -> str | None:
    return next(
        (c for c in df.columns if c.lower() == "branch"),
        None
    )


def _find_plan_branch_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        col_lower = col.lower()
        if "branch" in col_lower and "id" in col_lower:
            if df[col].notna().any() and (df[col].astype(str).str.strip() != "").any():
                return col

    for col in df.columns:
        if col.lower() in ("site", "sitio"):
            if df[col].notna().any():
                return col

    for col in df.columns:
        if col.lower() == "branch":
            if df[col].notna().any():
                return col

    return None


def _select_employee_branch_column(
    emp_branch_id: str | None,
    emp_branch_name: str | None,
    plan_branch_col: str
) -> str | None:
    plan_col_is_name = plan_branch_col.lower() == "branch"
    if plan_col_is_name:
        return emp_branch_name or emp_branch_id
    return emp_branch_id or emp_branch_name


def _find_null_columns_to_fill(
    merged_df: pd.DataFrame,
    plan_data_df: pd.DataFrame
) -> list[str]:
    return [
        c for c in merged_df.columns
        if merged_df[c].isna().all() and c in plan_data_df.columns
    ]


def _perform_branch_enrichment(
    merged_df: pd.DataFrame,
    plan_data_df: pd.DataFrame,
    emp_branch_col: str,
    plan_branch_col: str,
    null_cols: list[str]
) -> pd.DataFrame:
    result = merged_df.copy()
    plan_data_temp = plan_data_df.copy()

    result["_branch_merge"] = normalize_branch(result[emp_branch_col])
    plan_data_temp["_branch_merge"] = normalize_branch(plan_data_temp[plan_branch_col])

    branch_data = plan_data_temp[["_branch_merge"] + null_cols].drop_duplicates(
        subset=["_branch_merge"]
    )

    result = result.drop(columns=null_cols, errors="ignore")
    result = result.merge(branch_data, on="_branch_merge", how="left")
    result = result.drop(columns=["_branch_merge"], errors="ignore")

    logger.info(
        f"Enriched with branch data: filled {len(null_cols)} columns "
        f"via {emp_branch_col} -> {plan_branch_col}"
    )
    return result


def _merge_by_single_key(
    employees_df: pd.DataFrame,
    plan_data_df: pd.DataFrame,
    emp_branch_id: str | None,
    emp_branch_name: str | None,
    plan_branch_id: str | None,
    plan_branch_name: str | None
) -> pd.DataFrame:
    employees_df = employees_df.copy()
    plan_data_df = plan_data_df.copy()

    if emp_branch_id and plan_branch_id:
        emp_col = emp_branch_id
        plan_col = plan_branch_id
    else:
        emp_col = emp_branch_name
        plan_col = plan_branch_name

    employees_df["_branch_merge"] = normalize_branch(employees_df[emp_col])
    plan_data_df["_branch_merge"] = normalize_branch(plan_data_df[plan_col])

    result = employees_df.merge(
        plan_data_df,
        on="_branch_merge",
        how="left",
        suffixes=("", "_plan")
    )

    result = result.drop(columns=["_branch_merge"], errors="ignore")

    logger.info(f"Merged by Branch ({emp_col} -> {plan_col}): {len(result)} rows")
    return result


def _merge_by_composite_key(
    employees_df: pd.DataFrame,
    plan_data_df: pd.DataFrame,
    emp_branch_id: str,
    emp_branch_name: str,
    plan_branch_id: str,
    plan_branch_name: str
) -> pd.DataFrame:
    employees_df = employees_df.copy()
    plan_data_df = plan_data_df.copy()

    employees_df["_branchid_key"] = employees_df[emp_branch_id].astype(str).str.strip().str.upper()
    employees_df["_branch_key"] = normalize_branch(employees_df[emp_branch_name])

    plan_data_df["_branchid_key"] = plan_data_df[plan_branch_id].astype(str).str.strip().str.upper()
    plan_data_df["_branch_key"] = normalize_branch(plan_data_df[plan_branch_name])

    plan_cols = [
        c for c in plan_data_df.columns
        if c not in employees_df.columns and c not in ["_branchid_key", "_branch_key"]
    ]

    result = employees_df.merge(
        plan_data_df,
        on=["_branchid_key", "_branch_key"],
        how="left",
        suffixes=("", "_plan")
    )

    matched_count = result[plan_cols[0]].notna().sum() if plan_cols else 0
    logger.info(f"Merge by composite key (branchid + branch): {matched_count} matched")

    if matched_count == 0:
        result = _fallback_merge_by_branchid_only(
            employees_df, plan_data_df, plan_cols
        )
    elif plan_cols:
        result = _fill_unmatched_by_branchid(
            result, plan_data_df, plan_cols
        )

    result = result.drop(columns=["_branchid_key", "_branch_key"], errors="ignore")

    return result


def _fallback_merge_by_branchid_only(
    employees_df: pd.DataFrame,
    plan_data_df: pd.DataFrame,
    plan_cols: list[str]
) -> pd.DataFrame:
    logger.info("Composite merge found 0 matches, falling back to branchid-only merge")

    result = employees_df.merge(
        plan_data_df.drop(columns=["_branch_key"], errors="ignore"),
        on="_branchid_key",
        how="left",
        suffixes=("", "_plan")
    )

    matched_count = result[plan_cols[0]].notna().sum() if plan_cols else 0
    logger.info(f"Fallback merge by branchid only: {matched_count} matched")

    if matched_count == 0:
        result = _fallback_merge_by_branch_name_only(
            employees_df, plan_data_df, plan_cols
        )

    return result


def _fallback_merge_by_branch_name_only(
    employees_df: pd.DataFrame,
    plan_data_df: pd.DataFrame,
    plan_cols: list[str]
) -> pd.DataFrame:
    logger.info("Branchid merge found 0 matches, falling back to branch name merge")

    result = employees_df.merge(
        plan_data_df.drop(columns=["_branchid_key"], errors="ignore"),
        on="_branch_key",
        how="left",
        suffixes=("", "_plan")
    )

    matched_count = result[plan_cols[0]].notna().sum() if plan_cols else 0
    logger.info(f"Fallback merge by branch name: {matched_count} matched")

    return result


def _fill_unmatched_by_branchid(
    result: pd.DataFrame,
    plan_data_df: pd.DataFrame,
    plan_cols: list[str]
) -> pd.DataFrame:
    """Fill unmatched employees by branchid only (ignoring branch name mismatch)."""
    if not plan_cols:
        return result

    first_plan_col = plan_cols[0]
    unmatched_mask = result[first_plan_col].isna()
    unmatched_count = unmatched_mask.sum()

    if unmatched_count == 0:
        return result

    plan_lookup = plan_data_df.drop(columns=["_branch_key"], errors="ignore")
    plan_lookup = plan_lookup.drop_duplicates(subset=["_branchid_key"])

    for col in plan_cols:
        if col in result.columns and col in plan_lookup.columns:
            branchid_to_value = dict(zip(
                plan_lookup["_branchid_key"],
                plan_lookup[col],
                strict=False,
            ))

            result.loc[unmatched_mask, col] = result.loc[unmatched_mask, "_branchid_key"].map(
                branchid_to_value
            )

    filled_count = result[first_plan_col].notna().sum() - (len(result) - unmatched_count)
    if filled_count > 0:
        logger.info(f"Filled {filled_count} unmatched employees by branchid only")

    return result
