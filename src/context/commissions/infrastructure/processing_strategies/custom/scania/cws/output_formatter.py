from datetime import datetime

import pandas as pd


class CWSOutputFormatter:
    COLUMN_MAPPINGS = {
        "stock_admin_payment": "Pago Stock Admin",
        "fleet_availability_payment": "Pago Disponibilidad Flota",
        "open_work_orders_payment": "Pago OT Abiertas",
        "sales_compliance": "Cumplimiento Ventas",
        "sales_payment": "Pago Ventas",
        "efficiency": "Eficiencia",
        "efficiency_payment": "Pago Eficiencia",
        "productivity": "Productividad",
        "productivity_payment": "Pago Productividad",
        "team_factor": "Factor Equipo",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    }

    COLUMN_TYPES = {
        "Fecha": "date",
        "Rep ID": "text",
        "ID Transacción": "text",
        "Pago Stock Admin": "money",
        "Pago Disponibilidad Flota": "money",
        "Pago OT Abiertas": "money",
        "Cumplimiento Ventas": "percentage",
        "Pago Ventas": "money",
        "Eficiencia": "percentage",
        "Pago Eficiencia": "money",
        "Productividad": "percentage",
        "Pago Productividad": "money",
        "Factor Equipo": "percentage",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    }

    OUTPUT_COLUMNS = [
        "Fecha",
        "Rep ID",
        "ID Transacción",
        "Pago Stock Admin",
        "Pago Disponibilidad Flota",
        "Pago OT Abiertas",
        "Cumplimiento Ventas",
        "Pago Ventas",
        "Eficiencia",
        "Pago Eficiencia",
        "Productividad",
        "Pago Productividad",
        "Factor Equipo",
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

        numeric = ["Comisión", "Monto Final"]
        for col in numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df.fillna("")
