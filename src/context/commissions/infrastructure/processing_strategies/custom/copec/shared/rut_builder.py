import numpy as np
import pandas as pd


class RutBuilder:

    CLIENT_RUT_COL = "rut_cliente"
    CLIENT_DV_COL = "dv_cliente"
    EXEC_RUT_COL = "rut_ejecutivo"
    EXEC_DV_COL = "dv_ejecutivo"
    EXEC_ID_COL = "ejecutivo"

    CLIENT_RUT_COMPLETE = "client_rut_complete"
    EXEC_RUT_COMPLETE = "executive_rut_complete"
    REP_ID = "rep_id"

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._build_client_rut(df)
        df = self._build_executive_rut(df)
        df = self._build_rep_id(df)
        return df

    def _build_client_rut(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.CLIENT_RUT_COL not in df.columns or self.CLIENT_DV_COL not in df.columns:
            raise ValueError(f"Columns '{self.CLIENT_RUT_COL}' and '{self.CLIENT_DV_COL}' required")

        rut_str = df[self.CLIENT_RUT_COL].astype(str)
        already_has_dv = rut_str.str.contains("-")

        df[self.CLIENT_RUT_COMPLETE] = np.where(
            already_has_dv,
            rut_str,
            rut_str + "-" + df[self.CLIENT_DV_COL].astype(str)
        )
        return df

    def _build_executive_rut(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.EXEC_RUT_COL in df.columns and self.EXEC_DV_COL in df.columns:
            rut_str = df[self.EXEC_RUT_COL].astype(str)
            already_has_dv = rut_str.str.contains("-")

            df[self.EXEC_RUT_COMPLETE] = np.where(
                already_has_dv,
                rut_str,
                rut_str + "-" + df[self.EXEC_DV_COL].astype(str)
            )
        return df

    def _build_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.EXEC_RUT_COL in df.columns and self.EXEC_DV_COL in df.columns:
            df[self.REP_ID] = df[self.EXEC_RUT_COMPLETE]
            is_without_info = df[self.EXEC_RUT_COL].astype(str).str.upper().str.strip() == 'SIN_INFORMACION'
            exec_id = pd.to_numeric(df[self.EXEC_ID_COL], errors='coerce')
            df[self.REP_ID] = np.where(
                is_without_info & (exec_id == 1000491),
                '13573543-4', df[self.REP_ID]
            )
            df[self.REP_ID] = np.where(
                is_without_info & (exec_id == 1000627),
                '11796672-0', df[self.REP_ID]
            )
        return df

    def extract_client_ruts(self, df: pd.DataFrame, product_type: str | list[str]) -> set[str]:
        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()

        if "producto" not in df.columns:
            return set()

        if isinstance(product_type, list):
            product_types = [p.upper() for p in product_type]
        else:
            product_types = [product_type.upper()]

        product_col_normalized = df["producto"].str.upper().str.strip()
        product_df = df[product_col_normalized.isin(product_types)].copy()

        if "volumen" in product_df.columns:
            product_df["volumen"] = pd.to_numeric(product_df["volumen"], errors="coerce").fillna(0)
            product_df = product_df[product_df["volumen"] > 0]

        if self.CLIENT_RUT_COL in product_df.columns and self.CLIENT_DV_COL in product_df.columns:
            rut_str = product_df[self.CLIENT_RUT_COL].astype(str)
            already_has_dv = rut_str.str.contains("-")
            ruts = np.where(
                already_has_dv,
                rut_str,
                rut_str + "-" + product_df[self.CLIENT_DV_COL].astype(str)
            )
            return set(pd.Series(ruts).unique())

        return set()
