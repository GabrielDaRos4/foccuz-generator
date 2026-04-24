import logging

import pandas as pd

logger = logging.getLogger(__name__)

PLAN_TABS = [
    "PLAN_800",
    "PLAN_806",
    "PLAN_835",
    "PLAN_836",
    "PLAN_837",
    "PLAN_838",
    "PLAN_839",
    "PLAN_842",
    "PLAN_786",
    "PLAN_856",
]


def copec_summary_merge(dataframes: dict[str, pd.DataFrame], config: dict = None) -> pd.DataFrame:
    logger.info(f"copec_summary_merge received dataframes with keys: {list(dataframes.keys())}")

    sources = {}

    ejecutivos_df = dataframes.get("ejecutivos")
    if ejecutivos_df is not None and not ejecutivos_df.empty:
        sources["ejecutivos"] = ejecutivos_df
        logger.info(f"Loaded ejecutivos: {len(ejecutivos_df)} rows")

    for plan_name in PLAN_TABS:
        plan_key = plan_name.lower()
        plan_df = dataframes.get(plan_key)

        if plan_df is not None and not plan_df.empty:
            sources[plan_name] = plan_df
            logger.info(f"Loaded {plan_name}: {len(plan_df)} rows, columns: {list(plan_df.columns)}")
        else:
            logger.warning(f"No data found for {plan_name} (looked for key: '{plan_key}')")

    plan_sources = {k: v for k, v in sources.items() if k != "ejecutivos"}
    if not plan_sources:
        available_keys = list(dataframes.keys())
        non_empty_count = sum(1 for df in dataframes.values() if df is not None and not df.empty)
        raise ValueError(
            f"No plan data found for summary. "
            f"Available keys: {available_keys}. "
            f"Non-empty dataframes: {non_empty_count}/{len(dataframes)}. "
            f"Make sure the plan tabs have data in the Google Sheet."
        )

    result = pd.DataFrame({"_placeholder": [1]})
    result.attrs["sources"] = sources

    if config and "target_period" in config:
        result.attrs["target_period"] = config["target_period"]

    return result
