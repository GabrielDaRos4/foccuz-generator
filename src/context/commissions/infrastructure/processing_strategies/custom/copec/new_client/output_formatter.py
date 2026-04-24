from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta


class CopecOutputFormatter:
    COLUMN_MAPPINGS = {
        "volumen": "Volumen L",
        "descuento": "Descuento $/L",
        "unit_commission": "Comision $/L",
        "commission_amount": "Comisión Volumen Nuevo U3M",
        "new_client_bonus": "Bono Cliente Nuevo",
        "total_commission": "Comision",
        "rep_id": "Rep ID",
        "executive_rut_complete": "Rut Ejecutivo",
        "nombre_ejecutivo": "Nombre Ejecutivo",
        "client_rut_complete": "Rut Cliente",
        "oficina": "Site_ID",
        "descripcion_oficina_de_ventas": "Oficina",
        "jefe_de_ventas": "Jefatura",
        "date": "Fecha",
        "transaction_id": "ID Transaccion",
        "payment_month": "Mes Pago",
        "period": "Periodo",
        "client_type": "Nuevo Mes"
    }

    COLUMN_TYPES = {
        "Fecha": "date",
        "Rep ID": "text",
        "ID Transaccion": "text",
        "Rut Cliente": "text",
        "Nombre Cliente": "text",
        "Rut Ejecutivo": "text",
        "Nombre Ejecutivo": "text",
        "Site_ID": "text",
        "Oficina": "text",
        "Jefatura": "text",
        "Mes Pago": "text",
        "Periodo": "text",
        "Nuevo Mes": "text",
        "Volumen L": "integer",
        "Descuento $/L": "number",
        "Comision $/L": "number",
        "Comisión Volumen Nuevo U3M": "money",
        "Bono Cliente Nuevo": "money",
        "Comision": "money"
    }

    OUTPUT_COLUMNS_WITH_BONUS = [
        "Fecha", "Rep ID", "ID Transaccion", "Rut Cliente", "Nombre Cliente",
        "Volumen L", "Descuento $/L", "Comision $/L", "Comisión Volumen Nuevo U3M",
        "Bono Cliente Nuevo", "Comision", "Nuevo Mes"
    ]

    OUTPUT_COLUMNS_WITHOUT_BONUS = [
        "Fecha", "Rep ID", "ID Transaccion", "Rut Cliente", "Nombre Cliente",
        "Volumen L", "Descuento $/L", "Comision $/L", "Comisión Volumen Nuevo U3M",
        "Comision", "Nuevo Mes"
    ]

    def __init__(self, include_bonus_column: bool = True):
        self._include_bonus_column = include_bonus_column

    @property
    def output_columns(self) -> list[str]:
        if self._include_bonus_column:
            return self.OUTPUT_COLUMNS_WITH_BONUS
        return self.OUTPUT_COLUMNS_WITHOUT_BONUS

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
        df["period"] = period.strftime("%b/%Y")

        payment = period + relativedelta(months=1)
        df["payment_month"] = payment.strftime("%b/%Y")

        df["transaction_id"] = (
                df["date"].astype(str) + "_" +
                df["rep_id"].astype(str) + "_" +
                df["client_rut_complete"].astype(str)
        )
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_dict = {k: v for k, v in self.COLUMN_MAPPINGS.items() if k in df.columns}
        df = df.rename(columns=rename_dict)

        for col in df.columns:
            if "nombre" in col.lower() and "cliente" in col.lower():
                df = df.rename(columns={col: "Nombre Cliente"})
                break

        return df

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        existing = [c for c in self.output_columns if c in df.columns]
        return df[existing]

    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        required = ["Fecha", "Rep ID", "ID Transaccion", "Rut Cliente"]
        for col in required:
            if col in df.columns:
                df = df[df[col].notna() & (df[col] != "") & (df[col] != "nan")]

        numeric = [
            "Volumen L", "Descuento $/L", "Comision $/L",
            "Comisión Volumen Nuevo U3M", "Bono Cliente Nuevo", "Comision"
        ]
        for col in numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df.fillna("")


def extract_period(df: pd.DataFrame) -> datetime:
    if "anio" in df.columns and "mes" in df.columns and len(df) > 0:
        year = int(df["anio"].iloc[0])
        month = int(df["mes"].iloc[0])
        return datetime(year, month, 1)
    return datetime.now().replace(day=1)
