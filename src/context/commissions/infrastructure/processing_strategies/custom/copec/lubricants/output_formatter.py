from datetime import datetime

import pandas as pd


class LubricantsOutputFormatter:
    COLUMN_MAPPINGS = {
        "rep_id": "Rep ID",
        "commission": "Comision",
        "transaction_id": "ID Transaccion",
        "date": "Fecha",
        "solicitante": "Solicitante",
        "volumen": "Volumen L",
        "descuento": "Descuento %",
        "commission_per_liter": "Comision $/L",
        "cliente": "Cliente",
        "vendedor": "Vendedor",
    }

    COLUMN_TYPES = {
        "Fecha": "date",
        "Rep ID": "text",
        "ID Transaccion": "text",
        "Solicitante": "text",
        "Volumen L": "integer",
        "Descuento %": "percentage",
        "Comision $/L": "money",
        "Cliente": "text",
        "Vendedor": "text",
        "Comision": "money",
    }

    OUTPUT_COLUMNS = [
        "Fecha",
        "Rep ID",
        "ID Transaccion",
        "Solicitante",
        "Volumen L",
        "Descuento %",
        "Comision $/L",
        "Comision",
    ]

    def format(self, df: pd.DataFrame, period: datetime) -> pd.DataFrame:
        df = self._add_date_columns(df, period)
        df = self._rename_columns(df)
        df = self._select_output_columns(df)
        df = self._clean_data(df)
        df.attrs["column_types"] = self.COLUMN_TYPES
        return df

    @staticmethod
    def _add_date_columns(df: pd.DataFrame, period: datetime) -> pd.DataFrame:
        df["date"] = period.strftime("%Y-%m-%d")

        df["transaction_id"] = (
            df["date"].astype(str) + "_" +
            df["rep_id"].astype(str) + "_" +
            df["solicitante"].astype(str)
        )
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_dict = {k: v for k, v in self.COLUMN_MAPPINGS.items() if k in df.columns}
        return df.rename(columns=rename_dict)

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        existing = [c for c in self.OUTPUT_COLUMNS if c in df.columns]
        return df[existing]

    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        required = ["Fecha", "Rep ID", "ID Transaccion"]
        for col in required:
            if col in df.columns:
                df = df[df[col].notna() & (df[col] != "") & (df[col] != "nan")]

        numeric = ["Comision", "Volumen L", "Descuento %", "Comision $/L"]
        for col in numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df.fillna("")
