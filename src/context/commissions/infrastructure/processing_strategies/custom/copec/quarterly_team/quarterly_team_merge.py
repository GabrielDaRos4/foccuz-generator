import logging

import pandas as pd

logger = logging.getLogger(__name__)

SOURCE_PATTERNS = {
    'ejecutivos': ['ejecutivos', 'info-ejecutivos'],
    'TCT_TAE': ['tct_tae'],
    'CUPON_ELECTRONICO': ['cupon_electronico'],
    'APP_COPEC': ['app_copec'],
    'BLUEMAX': ['bluemax'],
    'TCT_PREMIUM': ['tct_premium'],
    'LUBRICANTES': ['lubricantes', 'detalle_comision', 'comision'],
    'POA_RESUMEN': ['poa_resumen', 'poa'],
}


def copec_quarterly_team_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}
    target_period = config.get('target_period')

    logger.info("=" * 60)
    logger.info(f"Quarterly Team merge - processing {len(dataframes)} sources")
    logger.info(f"Target period: {target_period}")
    logger.info(f"Available keys: {sorted(dataframes.keys())}")
    logger.info("=" * 60)

    sources_data = {}

    for source_name, patterns in SOURCE_PATTERNS.items():
        df = _collect_source_data(dataframes, patterns, source_name)
        if df is not None:
            sources_data[source_name] = df
            logger.info(f"Loaded {source_name}: {len(df)} rows")
        else:
            logger.warning(f"No data found for {source_name}")

    logger.info("=" * 60)
    logger.info(f"Sources loaded: {list(sources_data.keys())}")

    if 'ejecutivos' not in sources_data:
        raise ValueError("Ejecutivos data required for quarterly team calculation")

    if 'POA_RESUMEN' not in sources_data:
        raise ValueError("POA data required for quarterly team calculation")

    result_df = pd.DataFrame({'_placeholder': [1]})
    result_df.attrs['sources'] = sources_data
    result_df.attrs['target_period'] = target_period

    logger.info(f"Quarterly Team merge complete: {len(sources_data)} sources loaded")

    return result_df


def _collect_source_data(
    dataframes: dict[str, pd.DataFrame],
    patterns: list[str],
    source_name: str
) -> pd.DataFrame | None:
    matching_dfs = []

    for key, df in dataframes.items():
        if df is None or len(df) == 0:
            continue
        key_lower = key.lower()
        if any(pattern.lower() in key_lower for pattern in patterns):
            matching_dfs.append((key, df))
            logger.info(f"  Found {source_name} in: {key} ({len(df)} rows)")

    if not matching_dfs:
        return None

    return matching_dfs[0][1]
