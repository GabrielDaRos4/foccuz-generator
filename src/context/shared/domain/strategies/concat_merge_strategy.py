import logging

import pandas as pd

from .data_merge_strategy import DataMergeStrategy

logger = logging.getLogger(__name__)


class ConcatMergeStrategy(DataMergeStrategy):

    def merge(
        self,
        dataframes: dict[str, pd.DataFrame],
        config: dict[str, str | int | float | bool | list | dict]
    ) -> pd.DataFrame:
        axis = config.get('axis', 0)
        ignore_index = config.get('ignore_index', True)
        source_order = config.get('source_order', list(dataframes.keys()))

        missing = set(source_order) - set(dataframes.keys())
        if missing:
            raise KeyError(f"Sources {missing} not found in dataframes")

        dfs_to_concat = [dataframes[sid] for sid in source_order]

        logger.info(
            f"Concatenating {len(dfs_to_concat)} sources along axis={axis}: "
            f"{source_order}"
        )

        for sid, df in zip(source_order, dfs_to_concat, strict=True):
            logger.info(f"  - {sid}: {len(df)} rows, {len(df.columns)} columns")

        if axis in (0, '0', 'index'):
            result = pd.concat(dfs_to_concat, axis=0, ignore_index=bool(ignore_index))
        else:
            result = pd.concat(dfs_to_concat, axis=1, ignore_index=bool(ignore_index))

        logger.info(
            f"Concat completed: {len(result)} rows, {len(result.columns)} columns"
        )
        return result
