from datetime import datetime

import pandas as pd

MONTH_NAMES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}


class LicensePlateOutputFormatter:

    COLUMN_MAPPINGS = {
        "client_rut_complete": "Rut Cliente",
        "license_plate": "Patente",
        "new_client_bonus": "Bono Cliente Nuevo",
        "months_detail": "Meses Nuevo",
        "rep_id": "Rep ID",
        "date": "Fecha",
        "transaction_id": "ID Transaccion",
        "total_commission": "Comision"
    }

    COLUMN_TYPES = {
        "Fecha": "date",
        "Rep ID": "text",
        "ID Transaccion": "text",
        "Rut Cliente": "text",
        "Patente": "text",
        "Bono Cliente Nuevo": "money",
        "Comision": "money",
        "Meses Nuevo": "text"
    }

    OUTPUT_COLUMNS = [
        "Fecha",
        "Rep ID",
        "ID Transaccion",
        "Rut Cliente",
        "Patente",
        "Bono Cliente Nuevo",
        "Comision",
        "Meses Nuevo"
    ]

    def format(self, df: pd.DataFrame, period: datetime) -> pd.DataFrame:
        df = self._prepare_license_plate(df)
        df = self._add_date_columns(df, period)
        df = self._calculate_total(df)
        df = self._rename_columns(df)
        df = self._select_output_columns(df)
        df = self._clean_data(df)
        df.attrs["column_types"] = self.COLUMN_TYPES
        return df

    @staticmethod
    def _prepare_license_plate(df: pd.DataFrame) -> pd.DataFrame:
        df["license_plate"] = df["license_plate_normalized"]
        return df

    @staticmethod
    def _add_date_columns(df: pd.DataFrame, period: datetime) -> pd.DataFrame:
        df["date"] = period.strftime("%Y-%m-%d")

        df["transaction_id"] = (
            df["date"].astype(str) + "_" +
            df["client_rut_complete"].astype(str) + "_" +
            df["license_plate"].astype(str)
        )
        return df

    @staticmethod
    def _calculate_total(df: pd.DataFrame) -> pd.DataFrame:
        df["total_commission"] = df["new_client_bonus"]
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_dict = {k: v for k, v in self.COLUMN_MAPPINGS.items() if k in df.columns}
        return df.rename(columns=rename_dict)

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        existing = [c for c in self.OUTPUT_COLUMNS if c in df.columns]
        return df[existing]

    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        numeric = ["Bono Cliente Nuevo", "Comision"]
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


def format_month_name(month: int) -> str:
    return f"{month} - {MONTH_NAMES_ES.get(month, '')}"
