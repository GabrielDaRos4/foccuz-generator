import pandas as pd


def monedero_sales_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict
) -> pd.DataFrame:
    sales_df = dataframes.get("sales")

    if sales_df is None:
        raise ValueError("Sales dataframe is required")

    return sales_df.copy()
