import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.merge.normalizers import (
    normalize_rut,
)

logger = logging.getLogger(__name__)


def try_merge_by_rut(
    employees_df: pd.DataFrame,
    plan_data_df: pd.DataFrame
) -> pd.DataFrame | None:
    emp_rut_col = _find_employee_rut_column(employees_df)
    plan_rut_col = _find_plan_rut_column(plan_data_df)

    if not emp_rut_col or not plan_rut_col:
        return None

    employees_df = employees_df.copy()
    plan_data_df = plan_data_df.copy()

    plan_data_df = _aggregate_if_needed(plan_data_df, plan_rut_col)

    employees_df["_rut_merge"] = normalize_rut(employees_df[emp_rut_col])
    plan_data_df["_rut_merge"] = normalize_rut(plan_data_df[plan_rut_col])

    result = employees_df.merge(
        plan_data_df,
        on="_rut_merge",
        how="left",
        suffixes=("", "_plan")
    )

    result = result.drop(columns=["_rut_merge"], errors="ignore")

    logger.info(f"Merged by RUT: {len(result)} rows")
    return result


def _find_employee_rut_column(df: pd.DataFrame) -> str | None:
    return next((c for c in df.columns if "rut" in c.lower()), None)


def _find_plan_rut_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if col.lower() == "rut":
            return col

    rut_patterns = ["rut configuracion", "rut_configuracion", "ruttecnico", "rut tecnico"]
    for pattern in rut_patterns:
        for col in df.columns:
            if pattern in col.lower().replace(" ", ""):
                return col

    for col in df.columns:
        if "rut" in col.lower() and "cliente" not in col.lower():
            return col

    return None


def _aggregate_if_needed(df: pd.DataFrame, rut_col: str) -> pd.DataFrame:
    unique_ruts = df[rut_col].nunique()
    total_rows = len(df)

    if unique_ruts >= total_rows:
        return df

    transaction_patterns = ["factura", "chasis", "venta", "transaccion"]
    col_names_lower = " ".join(df.columns.str.lower())
    has_transaction_patterns = any(p in col_names_lower for p in transaction_patterns)

    if has_transaction_patterns:
        logger.info(
            f"Transaction data detected ({total_rows} rows, {unique_ruts} unique RUTs) - "
            f"preserving all rows for individual transaction processing"
        )
        return df

    config_patterns = ["configuracion", "estudio", "preventa"]
    has_config_patterns = any(p in col_names_lower for p in config_patterns)

    if has_config_patterns:
        logger.info(f"Aggregating {total_rows} rows to {unique_ruts} unique RUTs")

        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        non_numeric_cols = [c for c in df.columns if c not in numeric_cols and c != rut_col]

        agg_dict = {}
        for col in numeric_cols:
            agg_dict[col] = "sum"
        for col in non_numeric_cols:
            agg_dict[col] = "first"

        agg_dict["_config_count"] = (rut_col, "count")

        result = df.groupby(rut_col, as_index=False).agg(
            **{k: v if isinstance(v, tuple) else (k, v) for k, v in agg_dict.items()}
        )

        result = result.rename(columns={"_config_count": "configuraciones"})
        logger.info("Added 'configuraciones' column with counts per RUT")
        return result

    logger.info(f"Deduplicating {total_rows} rows to {unique_ruts} unique RUTs (keeping first)")
    return df.drop_duplicates(subset=[rut_col], keep="first")
