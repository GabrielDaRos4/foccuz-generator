import pandas as pd


def grupo_vanguardia_sales_merge(
    dataframes: dict[str, pd.DataFrame],
    config: dict
) -> pd.DataFrame:
    sales_df = dataframes.get("sales")
    consultants_df = dataframes.get("consultants")

    if sales_df is None:
        raise ValueError("Sales dataframe is required")

    result = sales_df.copy()

    if consultants_df is not None:
        result.attrs["consultants"] = consultants_df

    return result
