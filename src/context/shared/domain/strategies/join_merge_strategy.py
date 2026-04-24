import logging
from typing import cast

import pandas as pd

from .data_merge_strategy import DataMergeStrategy

logger = logging.getLogger(__name__)


class JoinMergeStrategy(DataMergeStrategy):

    def merge(
        self,
        dataframes: dict[str, pd.DataFrame],
        config: dict[str, str | int | float | bool | list | dict]
    ) -> pd.DataFrame:
        primary_id = str(config.get('primary_source', ''))
        joins = cast(list[dict[str, str | list[str]]], config.get('joins', []))

        self._validate_primary_source(primary_id, dataframes)

        result = dataframes[primary_id].copy()
        logger.info(f"Starting merge with primary source '{primary_id}': {len(result)} rows")

        for join_config in joins:
            result = self._apply_join(result, join_config, dataframes)

        logger.info(f"Merge completed: {len(result)} final rows")
        return result

    def _apply_join(
        self,
        left: pd.DataFrame,
        join_config: dict,
        dataframes: dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        source_id = join_config.get('source')
        on_columns = join_config.get('on', [])
        how = join_config.get('how', 'left')
        suffixes = join_config.get('suffixes', ('', f'_{source_id}'))

        self._validate_join_config(source_id, on_columns, dataframes)

        right_df = dataframes[source_id]
        self._validate_join_columns(on_columns, left, right_df, source_id)

        logger.info(
            f"Joining '{source_id}' ({len(right_df)} rows) "
            f"on {on_columns} with how='{how}'"
        )

        result = left.merge(right_df, on=on_columns, how=how, suffixes=suffixes)
        logger.info(f"After join: {len(result)} rows")
        return result

    @staticmethod
    def _validate_primary_source(primary_id: str, dataframes: dict) -> None:
        if not primary_id:
            raise ValueError("JoinMergeStrategy requires 'primary_source' in config")
        if primary_id not in dataframes:
            raise KeyError(f"Primary source '{primary_id}' not found in dataframes")

    @staticmethod
    def _validate_join_config(source_id: str, on_columns: list, dataframes: dict) -> None:
        if not source_id:
            raise ValueError("Join configuration must include 'source'")
        if source_id not in dataframes:
            raise KeyError(f"Join source '{source_id}' not found in dataframes")
        if not on_columns:
            raise ValueError(f"Join with '{source_id}' must specify 'on' columns")

    @staticmethod
    def _validate_join_columns(
        on_columns: list, left: pd.DataFrame, right: pd.DataFrame, source_id: str
    ) -> None:
        missing_left = set(on_columns) - set(left.columns)
        if missing_left:
            raise ValueError(f"Join columns {missing_left} not found in left dataframe")

        missing_right = set(on_columns) - set(right.columns)
        if missing_right:
            raise ValueError(
                f"Join columns {missing_right} not found in "
                f"right dataframe ('{source_id}')"
            )
