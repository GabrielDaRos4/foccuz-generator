import logging

import pandas as pd

logger = logging.getLogger(__name__)


def grupok_sales_advisor_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}

    employees_key = config.get('employees_key', 'employees')
    sales_key = config.get('sales_key', 'sales')
    tiers_key = config.get('tiers_key', 'commission_tiers')

    sales_df = dataframes.get(sales_key)
    if sales_df is None or sales_df.empty:
        raise ValueError(f"Sales data not found (key: {sales_key})")

    logger.info(f"Processing Grupo K sales data: {len(sales_df)} rows")

    employees_df = dataframes.get(employees_key)
    if employees_df is not None and not employees_df.empty:
        logger.info(f"Employees data loaded: {len(employees_df)} employees")
        sales_df.attrs['employees'] = employees_df
    else:
        logger.warning("Employees data not found")

    tiers_df = dataframes.get(tiers_key)
    if tiers_df is not None and not tiers_df.empty:
        logger.info(f"Commission tiers loaded: {len(tiers_df)} tiers")
        sales_df.attrs['commission_tiers'] = tiers_df
    else:
        logger.warning("Commission tiers not found")

    sales_df.attrs['sales'] = sales_df.copy()

    return sales_df


def grupok_store_manager_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}

    employees_key = config.get('employees_key', 'employees')
    sales_key = config.get('sales_key', 'sales')
    tiers_key = config.get('tiers_key', 'commission_tiers')

    sales_df = dataframes.get(sales_key)
    if sales_df is None or sales_df.empty:
        raise ValueError(f"Sales data not found (key: {sales_key})")

    logger.info(f"Processing Grupo K Store Manager sales data: {len(sales_df)} rows")

    employees_df = dataframes.get(employees_key)
    if employees_df is not None and not employees_df.empty:
        logger.info(f"Employees data loaded: {len(employees_df)} employees")
        sales_df.attrs['employees'] = employees_df
    else:
        logger.warning("Employees data not found")

    tiers_df = dataframes.get(tiers_key)
    if tiers_df is not None and not tiers_df.empty:
        logger.info(f"Commission tiers loaded: {len(tiers_df)} tiers")
        sales_df.attrs['commission_tiers'] = tiers_df
    else:
        logger.warning("Commission tiers not found")

    sales_df.attrs['sales'] = sales_df.copy()

    return sales_df


def grupok_product_manager_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}

    employees_key = config.get('employees_key', 'employees')
    sales_key = config.get('sales_key', 'sales')
    tiers_key = config.get('tiers_key', 'commission_tiers')

    sales_df = dataframes.get(sales_key)
    if sales_df is None or sales_df.empty:
        raise ValueError(f"Sales data not found (key: {sales_key})")

    logger.info(f"Processing Grupo K Product Manager sales data: {len(sales_df)} rows")

    employees_df = dataframes.get(employees_key)
    if employees_df is not None and not employees_df.empty:
        logger.info(f"Employees data loaded: {len(employees_df)} employees")
        sales_df.attrs['employees'] = employees_df
    else:
        logger.warning("Employees data not found")

    tiers_df = dataframes.get(tiers_key)
    if tiers_df is not None and not tiers_df.empty:
        logger.info(f"Commission tiers loaded: {len(tiers_df)} tiers")
        sales_df.attrs['commission_tiers'] = tiers_df
    else:
        logger.warning("Commission tiers not found")

    sales_df.attrs['sales'] = sales_df.copy()

    return sales_df


def grupok_subgerente_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}

    employees_key = config.get('employees_key', 'employees')
    sales_key = config.get('sales_key', 'sales')
    tiers_key = config.get('tiers_key', 'commission_tiers')

    sales_df = dataframes.get(sales_key)
    if sales_df is None or sales_df.empty:
        raise ValueError(f"Sales data not found (key: {sales_key})")

    logger.info(f"Processing Grupo K Subgerente sales data: {len(sales_df)} rows")

    employees_df = dataframes.get(employees_key)
    if employees_df is not None and not employees_df.empty:
        logger.info(f"Employees data loaded: {len(employees_df)} employees")
        sales_df.attrs['employees'] = employees_df
    else:
        logger.warning("Employees data not found")

    tiers_df = dataframes.get(tiers_key)
    if tiers_df is not None and not tiers_df.empty:
        logger.info(f"Commission tiers loaded: {len(tiers_df)} tiers")
        sales_df.attrs['commission_tiers'] = tiers_df
    else:
        logger.warning("Commission tiers not found")

    sales_df.attrs['sales'] = sales_df.copy()

    return sales_df
