import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

SOURCE_PATTERNS = {
    'TCT_TAE': ['tct_tae'],
    'CUPON_ELECTRONICO': ['cupon_electronico'],
    'APP_COPEC': ['app_copec'],
    'BLUEMAX': ['bluemax'],
    'TCT_PREMIUM': ['tct_premium'],
    'LUBRICANTES': ['lubricantes', 'detalle_comision', 'comision'],
    'POA_RESUMEN': ['poa_resumen', 'poa'],
    'BONUS': ['bonus'],
}


def _extract_period_from_df(df: pd.DataFrame) -> tuple[str, str] | tuple[None, None]:
    df_copy = df.copy()
    df_copy.columns = df_copy.columns.str.lower().str.strip()

    if 'anio' in df_copy.columns and 'mes' in df_copy.columns and len(df_copy) > 0:
        year = str(int(df_copy['anio'].iloc[0]))
        month = str(int(df_copy['mes'].iloc[0])).zfill(2)
        return year, month

    return None, None


def _filter_by_target_period(
    dataframes: list[tuple[str, pd.DataFrame]],
    target_period: str
) -> pd.DataFrame | None:
    try:
        target_date = datetime.strptime(target_period, '%Y-%m-%d')
        target_year = str(target_date.year)
        target_month = str(target_date.month).zfill(2)

        for key, df in dataframes:
            year, month = _extract_period_from_df(df)
            logger.info(f"  Checking {key}: period={year}-{month}, target={target_year}-{target_month}")
            if year == target_year and month == target_month:
                logger.info(f"  -> MATCH: Found data for period {target_year}-{target_month} in {key}")
                return df

        logger.warning(f"No exact match for target period {target_period}")

        dfs_without_period = [(k, df) for k, df in dataframes if _extract_period_from_df(df) == (None, None)]
        if dfs_without_period:
            logger.info(f"Using data without period columns: {dfs_without_period[0][0]}")
            return dfs_without_period[0][1]

        logger.info("Using most recent available data")
        return dataframes[0][1] if dataframes else None

    except ValueError as e:
        logger.error(f"Invalid target_period format: {e}")
        return dataframes[0][1] if dataframes else None


def _collect_source_data(
    dataframes: dict[str, pd.DataFrame],
    patterns: list[str],
    target_period: str,
    source_name: str
) -> pd.DataFrame | None:
    matching_dfs = []

    logger.info(f"Looking for {source_name} with patterns: {patterns}")

    for key, df in dataframes.items():
        key_lower = key.lower()
        if any(pattern.lower() in key_lower for pattern in patterns) and df is not None and len(df) > 0:
            matching_dfs.append((key, df))
            year, month = _extract_period_from_df(df)
            logger.info(f"  Found: {key} ({len(df)} rows, period={year}-{month})")

    if not matching_dfs:
        logger.warning(f"  No dataframes found matching patterns {patterns}")
        return None

    if target_period:
        return _filter_by_target_period(matching_dfs, target_period)

    return matching_dfs[0][1] if matching_dfs else None


def copec_poa_compliance_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}
    target_period = config.get('target_period')

    logger.info("=" * 60)
    logger.info(f"POA Compliance merge - processing {len(dataframes)} sources")
    logger.info(f"Target period: {target_period}")
    logger.info(f"Available keys: {sorted(dataframes.keys())}")
    logger.info("=" * 60)

    sources_data = {}

    for source_name, patterns in SOURCE_PATTERNS.items():
        df = _collect_source_data(dataframes, patterns, target_period, source_name)
        if df is not None:
            sources_data[source_name] = df
            logger.info(f"✓ Loaded {source_name}: {len(df)} rows")
        else:
            logger.warning(f"✗ No data found for {source_name}")

    logger.info("=" * 60)
    logger.info(f"Sources loaded: {list(sources_data.keys())}")

    if not sources_data:
        raise ValueError("No source data found for POA compliance calculation")

    result_df = pd.DataFrame({'_placeholder': [1]})
    result_df.attrs['sources'] = sources_data
    result_df.attrs['target_period'] = target_period

    logger.info(f"POA Compliance merge complete: {len(sources_data)} sources loaded")

    return result_df
