import logging

import pandas as pd

logger = logging.getLogger(__name__)

SHEET_KEYS = {
    'MONTHLY_COMMISSION': 'comision_venta_mensual',
    'DETAIL': 'detalle_comision_mes',
}


def copec_lubricants_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict = None
) -> pd.DataFrame:
    config = config or {}

    monthly_key = SHEET_KEYS['MONTHLY_COMMISSION']
    detail_key = SHEET_KEYS['DETAIL']

    detail_df = dataframes.get(detail_key)

    if detail_df is None or detail_df.empty:
        raise ValueError(f"Detail commission data not found (key: {detail_key})")

    logger.info(f"Processing lubricants data: {len(detail_df)} detail rows")

    monthly_df = dataframes.get(monthly_key)
    if monthly_df is not None:
        logger.info(f"Monthly commission summary: {len(monthly_df)} rows")
        detail_df.attrs['monthly_summary'] = monthly_df

    return detail_df
