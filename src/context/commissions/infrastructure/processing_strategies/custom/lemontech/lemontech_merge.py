import logging

import pandas as pd

logger = logging.getLogger(__name__)


def lemontech_monthly_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    logger.info(f"lemontech_monthly_merge received dataframes with keys: {list(dataframes.keys())}")

    deals_df = dataframes.get('deals')
    goals_df = dataframes.get('goals')

    if deals_df is None or deals_df.empty:
        raise ValueError("No deals data found for Lemontech monthly merge")

    if goals_df is None or goals_df.empty:
        raise ValueError("No goals data found for Lemontech monthly merge")

    logger.info(f"Loaded deals: {len(deals_df)} rows")
    logger.info(f"Loaded goals: {len(goals_df)} rows")

    result = deals_df.copy()
    result.attrs['goals'] = goals_df

    if config and 'target_period' in config:
        result.attrs['target_period'] = config['target_period']

    return result


def lemontech_quarterly_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    logger.info(f"lemontech_quarterly_merge received dataframes with keys: {list(dataframes.keys())}")

    deals_df = dataframes.get('deals')
    goals_df = dataframes.get('goals')

    if deals_df is None or deals_df.empty:
        raise ValueError("No deals data found for Lemontech quarterly merge")

    if goals_df is None or goals_df.empty:
        raise ValueError("No goals data found for Lemontech quarterly merge")

    logger.info(f"Loaded deals: {len(deals_df)} rows")
    logger.info(f"Loaded goals: {len(goals_df)} rows")

    result = deals_df.copy()
    result.attrs['goals'] = goals_df

    if config and 'target_period' in config:
        result.attrs['target_period'] = config['target_period']

    return result
