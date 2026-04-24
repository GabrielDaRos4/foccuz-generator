import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

COLUMN_KEYS = {
    'YEAR': 'anio',
    'MONTH': 'mes',
}

DATAFRAME_KEYS = {
    'CURRENT_SALES': 'ventas_mes_actual',
    'EMPLOYEES': 'empleados',
    'HISTORICAL_SALES_PREFIX': 'ventas_mes_'
}

DATAFRAME_ATTRS = {
    'HISTORICAL_SALES': 'ventas_historicas',
    'EMPLOYEES': 'empleados'
}


def _extract_period_from_df(df: pd.DataFrame) -> tuple[str, str] | tuple[None, None]:
    df_copy = df.copy()
    df_copy.columns = df_copy.columns.str.lower().str.strip()

    year_col = COLUMN_KEYS['YEAR']
    month_col = COLUMN_KEYS['MONTH']

    if year_col in df_copy.columns and month_col in df_copy.columns and len(df_copy) > 0:
        year = str(df_copy[year_col].iloc[0])
        month = str(df_copy[month_col].iloc[0]).zfill(2)
        return year, month

    return None, None


def _sort_dataframes_by_date(
    dataframes: list[tuple[str, pd.DataFrame]]
) -> list[tuple[datetime, pd.DataFrame, str]]:
    dated_dfs = []

    for key, df in dataframes:
        year, month = _extract_period_from_df(df)

        if year and month:
            try:
                period_date = datetime(int(year), int(month), 1)
                dated_dfs.append((period_date, df, key))
                logger.debug(f"DataFrame {key}: {year}-{month}")
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse date from {key}: {e}")
                dated_dfs.append((datetime(1900, 1, 1), df, key))
        else:
            logger.warning(f"No date columns found in {key}")
            dated_dfs.append((datetime(1900, 1, 1), df, key))

    dated_dfs.sort(key=lambda x: x[0], reverse=True)

    return dated_dfs


def copec_new_client_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}

    current_sales_key = DATAFRAME_KEYS['CURRENT_SALES']
    employees_key = DATAFRAME_KEYS['EMPLOYEES']
    historical_prefix = DATAFRAME_KEYS['HISTORICAL_SALES_PREFIX']

    employees_df = dataframes.get(employees_key)
    target_period = config.get('target_period')

    all_sales_dfs = []
    for key in sorted(dataframes.keys()):
        if key.startswith(historical_prefix) or key == current_sales_key:
            if dataframes[key] is not None and len(dataframes[key]) > 0:
                all_sales_dfs.append((key, dataframes[key]))

    if not all_sales_dfs:
        raise ValueError("No sales data found in dataframes")

    logger.info(f"Found {len(all_sales_dfs)} sales datasets to process")

    if target_period:
        current_sales, historical_sales = _process_with_target_period(
            all_sales_dfs, target_period
        )
    else:
        logger.info("No target_period provided, using first file as current sales")
        current_sales = all_sales_dfs[0][1]
        historical_sales = [df for _, df in all_sales_dfs[1:]]

    result_df = current_sales.copy()
    result_df.attrs[DATAFRAME_ATTRS['HISTORICAL_SALES']] = historical_sales
    result_df.attrs[DATAFRAME_ATTRS['EMPLOYEES']] = employees_df

    logger.info(f"Merge complete: {len(result_df)} current rows, {len(historical_sales)} historical datasets")
    _log_historical_periods(historical_sales)

    return result_df


def _process_with_target_period(
    all_sales_dfs: list[tuple[str, pd.DataFrame]],
    target_period: str
) -> tuple[pd.DataFrame, list[pd.DataFrame]]:
    try:
        target_date = datetime.strptime(target_period, '%Y-%m-%d')
        target_year = str(target_date.year)
        target_month = str(target_date.month).zfill(2)

        logger.info(f"Processing with target period: {target_year}-{target_month}")

        current_sales = None
        historical_list = []

        for key, df in all_sales_dfs:
            year, month = _extract_period_from_df(df)

            if year == target_year and month == target_month:
                current_sales = df
                logger.info(f"Selected {key} as current sales for period {target_year}-{target_month}")
            else:
                if year and month:
                    try:
                        period_date = datetime(int(year), int(month), 1)
                        if period_date < target_date:
                            historical_list.append((period_date, df, key))
                            logger.info(f"Added {key} to historical: {year}-{month}")
                        else:
                            logger.info(f"Skipping {key} ({year}-{month}) - after target period")
                    except (ValueError, TypeError):
                        pass

        historical_list.sort(key=lambda x: x[0], reverse=True)
        historical_sales = [df for _, df, _ in historical_list]

        if current_sales is None:
            logger.warning(
                f"No sales data found for target period {target_period}, "
                "using most recent file"
            )
            if all_sales_dfs:
                sorted_all = _sort_dataframes_by_date(all_sales_dfs)
                current_sales = sorted_all[0][1]
                historical_sales = [df for _, df, _ in sorted_all[1:]]
            else:
                raise ValueError("No sales data available")

        return current_sales, historical_sales

    except Exception as e:
        logger.error(f"Error processing target_period {target_period}: {e}")
        logger.warning("Falling back to default logic")
        return all_sales_dfs[0][1], [df for _, df in all_sales_dfs[1:]]


def _log_historical_periods(historical_sales: list[pd.DataFrame]) -> None:
    if not historical_sales:
        return

    logger.info("=" * 60)
    logger.info("Historical sales order (most recent to oldest):")

    for i, df in enumerate(historical_sales[:3]):
        year, month = _extract_period_from_df(df)
        period_label = f"M-{i+1}"

        if year and month:
            logger.info(f"  {period_label}: {year}-{month} ({len(df)} rows)")
        else:
            logger.info(f"  {period_label}: Unknown date ({len(df)} rows)")

    if len(historical_sales) > 3:
        logger.info(f"  ... and {len(historical_sales) - 3} more historical datasets")

    logger.info("=" * 60)
