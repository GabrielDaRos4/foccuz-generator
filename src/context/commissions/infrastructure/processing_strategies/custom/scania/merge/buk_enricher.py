import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.infrastructure.repositories.buk_data_repository import (
    BuKDataRepository,
)

logger = logging.getLogger(__name__)


def flatten_buk_nested_fields(df: pd.DataFrame, target_period: str = None) -> pd.DataFrame:
    result = df.copy()

    result = _ensure_employee_id(result)
    result = _ensure_cargo(result, target_period)
    result = _ensure_cargo2(result, target_period)
    result = _ensure_branch_id(result, target_period)
    result = _ensure_branch_name(result, target_period)
    result = _ensure_employee_status(result)
    result = _ensure_job_dates(result, target_period)

    return result


def enrich_with_payroll_details(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    buk_base_url = config.get("buk_base_url")
    buk_api_token = config.get("buk_api_token")
    payroll_period = config.get("target_period") or config.get("payroll_period")

    if not buk_base_url or not buk_api_token:
        logger.debug("BuK credentials not provided, skipping payroll enrichment")
        return df

    logger.info(f"Enriching with payroll details for period: {payroll_period}")

    try:
        buk_repo = BuKDataRepository()
        enriched_df = buk_repo.enrich_with_payroll_details(
            employees_df=df,
            base_url=buk_base_url,
            api_token=buk_api_token,
            period=payroll_period
        )
        return enriched_df
    except Exception as e:
        logger.warning(f"Failed to enrich with payroll details: {e}")
        return df


def _ensure_employee_id(df: pd.DataFrame) -> pd.DataFrame:
    if "id empleado" not in df.columns and "id" in df.columns:
        df["id empleado"] = df["id"]
        logger.info("Mapped 'id' to 'id empleado' for Rep ID")
    return df


def _ensure_cargo(df: pd.DataFrame, target_period: str = None) -> pd.DataFrame:
    if "cargo" not in df.columns and "jobs" in df.columns:
        df["cargo"] = df.apply(
            lambda row: _get_job_attribute_for_period(
                row.get("jobs"),
                target_period,
                ["role", "code"],
                row.get("current_job")
            ),
            axis=1
        )
        logger.info(f"Extracted 'cargo' from jobs array for period {target_period}")
    elif "cargo" not in df.columns:
        cargo_code = _extract_nested_value(df, "current_job", ["role", "code"])
        if cargo_code is not None:
            df["cargo"] = cargo_code
            logger.info("Extracted 'cargo' from current_job.role.code")
    return df


def _ensure_cargo2(df: pd.DataFrame, target_period: str = None) -> pd.DataFrame:
    if "cargo2" not in df.columns and "jobs" in df.columns:
        df["cargo2"] = df.apply(
            lambda row: _get_role_for_period(
                row.get("jobs"),
                target_period,
                row.get("current_job")
            ),
            axis=1
        )
        logger.info(f"Extracted 'cargo2' from jobs array for period {target_period}")
    elif "cargo2" not in df.columns:
        modelo_renta = _extract_nested_value(
            df, "current_job", ["custom_attributes", "Modelo Renta Variable"]
        )
        if modelo_renta is not None:
            df["cargo2"] = modelo_renta.fillna("").astype(str).str.strip()
            logger.info("Extracted 'cargo2' from current_job.custom_attributes.Modelo Renta Variable")
    return df


def _ensure_branch_id(df: pd.DataFrame, target_period: str = None) -> pd.DataFrame:
    if "branchid" not in df.columns and "branch_id" not in df.columns:
        if "jobs" in df.columns:
            df["branchid"] = df.apply(
                lambda row: _get_job_attribute_for_period(
                    row.get("jobs"),
                    target_period,
                    ["custom_attributes", "ubica"],
                    row.get("current_job")
                ),
                axis=1
            )
            logger.info(f"Extracted 'branchid' from jobs array for period {target_period}")
        else:
            branch_id = _extract_nested_value(df, "current_job", ["custom_attributes", "ubica"])
            if branch_id is not None:
                df["branchid"] = branch_id
                logger.info("Extracted 'branchid' from current_job.custom_attributes.ubica")
    return df


def _ensure_branch_name(df: pd.DataFrame, target_period: str = None) -> pd.DataFrame:
    if "branch" not in df.columns:
        if "jobs" in df.columns:
            df["branch"] = df.apply(
                lambda row: _get_job_attribute_for_period(
                    row.get("jobs"),
                    target_period,
                    ["custom_attributes", "d_convenio"],
                    row.get("current_job")
                ),
                axis=1
            )
            df["branch"] = df["branch"].fillna("").astype(str).str.strip()
            logger.info(f"Extracted 'branch' from jobs array for period {target_period}")
        else:
            d_convenio = _extract_nested_value(df, "current_job", ["custom_attributes", "d_convenio"])
            if d_convenio is not None:
                df["branch"] = d_convenio.fillna("").astype(str).str.strip()
                logger.info("Extracted 'branch' from current_job.custom_attributes.d_convenio")
    return df


def _ensure_employee_status(df: pd.DataFrame) -> pd.DataFrame:
    if "employee_status" not in df.columns and "status" in df.columns:
        df["employee_status"] = df["status"].astype(str).str.strip().str.lower()
        logger.info("Mapped 'status' to 'employee_status'")
    return df


def _ensure_job_dates(df: pd.DataFrame, target_period: str = None) -> pd.DataFrame:
    if "jobs" in df.columns:
        if "job_start_date" not in df.columns:
            df["job_start_date"] = df.apply(
                lambda row: _get_job_attribute_for_period(
                    row.get("jobs"),
                    target_period,
                    ["start_date"],
                    row.get("current_job")
                ),
                axis=1
            )
            df["job_start_date"] = pd.to_datetime(df["job_start_date"], errors="coerce")
            logger.info(f"Extracted 'job_start_date' from jobs array for period {target_period}")

        if "job_end_date" not in df.columns:
            df["job_end_date"] = df.apply(
                lambda row: _get_job_attribute_for_period(
                    row.get("jobs"),
                    target_period,
                    ["end_date"],
                    row.get("current_job")
                ),
                axis=1
            )
            df["job_end_date"] = pd.to_datetime(df["job_end_date"], errors="coerce")
            logger.info(f"Extracted 'job_end_date' from jobs array for period {target_period}")
    else:
        if "job_start_date" not in df.columns:
            job_start = _extract_nested_value(df, "current_job", ["start_date"])
            if job_start is not None:
                df["job_start_date"] = pd.to_datetime(job_start, errors="coerce")
                logger.info("Extracted 'job_start_date' from current_job.start_date")

        if "job_end_date" not in df.columns:
            job_end = _extract_nested_value(df, "current_job", ["end_date"])
            if job_end is not None:
                df["job_end_date"] = pd.to_datetime(job_end, errors="coerce")
                logger.info("Extracted 'job_end_date' from current_job.end_date")

    return df


def _get_job_for_period(jobs: list, target_period: str) -> dict | None:
    if not jobs or not isinstance(jobs, list):
        return None

    target_date = None
    if target_period:
        try:
            target_date = datetime.strptime(target_period, "%Y-%m-%d")
        except ValueError:
            return None

    if not target_date:
        return None

    most_recent_ended_job = None
    most_recent_end_date = None

    for job in jobs:
        if not isinstance(job, dict):
            continue

        start_date_str = job.get("start_date")
        end_date_str = job.get("end_date")

        if not start_date_str:
            continue

        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None

            if start_date <= target_date and (end_date is None or target_date <= end_date):
                return job

            if end_date and end_date < target_date:
                if most_recent_end_date is None or end_date > most_recent_end_date:
                    most_recent_ended_job = job
                    most_recent_end_date = end_date
        except (ValueError, TypeError):
            continue

    if most_recent_ended_job:
        return most_recent_ended_job

    return None


def _get_job_attribute_for_period(
    jobs: list,
    target_period: str,
    attribute_path: list[str],
    fallback_job: dict = None
) -> str | None:
    job = _get_job_for_period(jobs, target_period)

    if job:
        value = _get_nested_value(job, attribute_path)
        if value is not None:
            return str(value).strip() if value else None

    if fallback_job and isinstance(fallback_job, dict):
        value = _get_nested_value(fallback_job, attribute_path)
        if value is not None:
            return str(value).strip() if value else None

    return None


def _get_role_for_period(jobs: list, target_period: str, current_job: dict = None) -> str:
    job = _get_job_for_period(jobs, target_period)

    if job:
        custom_attrs = job.get("custom_attributes", {})
        if custom_attrs:
            modelo = custom_attrs.get("Modelo Renta Variable", "")
            if modelo:
                return str(modelo).strip()

    if job and current_job and isinstance(current_job, dict):
        custom_attrs = current_job.get("custom_attributes", {})
        if custom_attrs:
            modelo = custom_attrs.get("Modelo Renta Variable", "")
            if modelo:
                return str(modelo).strip()

    return ""


def _get_nested_value(obj: dict, keys: list[str]):
    if obj is None or not isinstance(obj, dict):
        return None
    for key in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
        if obj is None:
            return None
    return obj


def _extract_nested_value(
    df: pd.DataFrame,
    column: str,
    path: list[str]
) -> pd.Series | None:
    if column not in df.columns:
        return None

    try:
        return df[column].apply(lambda x: _get_nested_value(x, path))
    except Exception:
        return None
