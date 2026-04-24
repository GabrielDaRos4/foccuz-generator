
import pandas as pd


class GocarOutputFormatter:

    COLUMN_TYPES = {
        "Fecha": "date",
        "Fecha de Pago": "date",
        "Rep ID": "text",
        "ID Transaccion": "text",
        "Negocio": "text",
        "Cliente": "text",
        "Chasis": "text",
        "Condiciones": "text",
        "Factura": "text",
        "Utilidad Bruta": "money",
        "% Comision": "percentage",
        "Comision Base": "money",
        "Toma": "money",
        "Financiamiento": "money",
        "Edegas": "money",
        "Verificacion": "money",
        "Accesorios": "money",
        "Garantias": "money",
        "Seguros": "money",
        "Placas": "money",
        "Bonos Otros": "money",
        "Descuentos": "money",
        "Semana": "integer",
        "Comision": "money",
    }

    OUTPUT_COLUMNS = [
        "Fecha",
        "Fecha de Pago",
        "Rep ID",
        "ID Transaccion",
        "Negocio",
        "Cliente",
        "Chasis",
        "Condiciones",
        "Factura",
        "Utilidad Bruta",
        "% Comision",
        "Comision Base",
        "Toma",
        "Financiamiento",
        "Edegas",
        "Verificacion",
        "Accesorios",
        "Garantias",
        "Seguros",
        "Placas",
        "Bonos Otros",
        "Descuentos",
        "Semana",
        "Comision",
    ]

    def format(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = self._select_output_columns(df)
        df = self._clean_data(df)
        df.attrs["column_types"] = self.COLUMN_TYPES
        return df

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        existing = [c for c in self.OUTPUT_COLUMNS if c in df.columns]
        return df[existing].copy()

    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        if "Rep ID" in df.columns:
            df = df[df["Rep ID"].notna()].copy()
            df["Rep ID"] = df["Rep ID"].astype(int).astype(str)

        money_columns = [
            "Utilidad Bruta", "Comision Base", "Toma", "Financiamiento",
            "Edegas", "Verificacion", "Accesorios", "Garantias", "Seguros",
            "Placas", "Bonos Otros", "Descuentos", "Comision"
        ]

        for col in money_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).round(2)

        if "Semana" in df.columns:
            df["Semana"] = pd.to_numeric(df["Semana"], errors="coerce").fillna(0).astype(int)

        if "% Comision" in df.columns:
            df["% Comision"] = pd.to_numeric(df["% Comision"], errors="coerce").fillna(0)

        return df.fillna("")
