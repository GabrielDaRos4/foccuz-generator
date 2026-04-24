from datetime import datetime

import pandas as pd


class ClaimsOutputFormatter:
    COLUMN_MAPPINGS = {
        "leadtime": "Lead Time",
        "leadtime_payment": "Pago Lead Time",
        "wip": "WIP",
        "wip_payment": "Pago WIP",
        "compliance": "Cumplimiento",
        "compliance_payment": "Pago Cumplimiento",
        "inventory_accuracy": "Exactitud Inventario",
        "inventory_payment": "Pago Inventario",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    COLUMN_TYPES = {
        "Fecha": "date",
        "Rep ID": "text",
        "ID Transacción": "text",
        "Lead Time": "decimal",
        "Pago Lead Time": "money",
        "WIP": "percentage",
        "Pago WIP": "money",
        "Cumplimiento": "percentage",
        "Pago Cumplimiento": "money",
        "Exactitud Inventario": "percentage",
        "Pago Inventario": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    OUTPUT_COLUMNS = [
        "Fecha",
        "Rep ID",
        "ID Transacción",
        "Lead Time",
        "Pago Lead Time",
        "WIP",
        "Pago WIP",
        "Cumplimiento",
        "Pago Cumplimiento",
        "Exactitud Inventario",
        "Pago Inventario",
        "Días Trabajados",
        "Monto Final",
        "Comisión",
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
            df["date"].astype(str) + "_" + df["rep_id"].astype(str)
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
        required = ["Fecha", "Rep ID", "ID Transacción"]
        for col in required:
            if col in df.columns:
                df = df[df[col].notna() & (df[col] != "") & (df[col] != "nan")]

        numeric = ["Comisión", "Monto Final", "Pago Lead Time", "Pago WIP"]
        for col in numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df.fillna("")
