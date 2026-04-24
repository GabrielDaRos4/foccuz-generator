import logging

import pandas as pd

logger = logging.getLogger(__name__)


def gocar_commission_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}

    sales_key = config.get('sales_key', 'sales')
    users_key = config.get('users_key', 'users_mapping')

    sales_df = dataframes.get(sales_key)
    if sales_df is None or sales_df.empty:
        raise ValueError(f"Sales data not found (key: {sales_key})")

    logger.info(f"Processing GOCAR sales data: {len(sales_df)} rows")

    users_df = dataframes.get(users_key)
    if users_df is not None and not users_df.empty:
        logger.info(f"Users mapping loaded: {len(users_df)} users")
        sales_df.attrs['users_mapping'] = users_df
    else:
        logger.warning("Users mapping not found")

    return sales_df
