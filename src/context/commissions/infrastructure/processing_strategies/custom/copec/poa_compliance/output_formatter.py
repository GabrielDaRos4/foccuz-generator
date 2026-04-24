from datetime import datetime

import pandas as pd

from .bonus_lookup import BonusLookup

METRIC_TYPE_VOLUMEN = "volumen"
METRIC_TYPE_MARGEN = "margen"

SPANISH_MONTHS = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
}


class PoaComplianceOutputFormatter:

    PRODUCT_ORDER = [
        "TCT",
        "TAE",
        "CE",
        "AppCE",
        "CE + AppCE",
        "BM TCT",
        "BM AppCE",
        "BM Total",
        "Lubricantes",
        "TCTP",
    ]

    PRODUCT_DISPLAY_NAMES = {
        "Bluemax TCT": "BM TCT",
        "Bluemax AppCE": "BM AppCE",
        "Bluemax Total": "BM Total",
    }

    def __init__(self, metric_type: str = METRIC_TYPE_VOLUMEN):
        self._metric_type = metric_type
        self._setup_columns()

    def _setup_columns(self):
        self._product_column = "Producto"
        self._value_column_base = "Real"

        self.COLUMN_TYPES = {
            "Fecha": "date",
            "Rep ID": "text",
            "ID Transaccion": "text",
            "Producto": "text",
            "Cumplimiento %": "percent",
            "Bono": "money",
            "Comision": "money",
        }

        self.OUTPUT_COLUMNS = [
            "Fecha",
            "Rep ID",
            "ID Transaccion",
            "Producto",
        ]

    def format(
        self,
        df: pd.DataFrame,
        period: datetime,
        bonus_df: pd.DataFrame = None
    ) -> pd.DataFrame:
        value_column = self._get_value_column_name(period)
        poa_column = self._get_poa_column_name(period)
        self.COLUMN_TYPES[value_column] = "number" if self._metric_type == METRIC_TYPE_MARGEN else "integer"
        self.COLUMN_TYPES[poa_column] = "number" if self._metric_type == METRIC_TYPE_MARGEN else "integer"

        output_columns = self.OUTPUT_COLUMNS.copy()
        output_columns.append(value_column)
        output_columns.append(poa_column)
        output_columns.append("Cumplimiento %")
        output_columns.append("Bono")
        output_columns.append("Comision")
        self._current_output_columns = output_columns

        self._bonus_lookup = BonusLookup(bonus_df, self._metric_type) if bonus_df is not None else None

        df = self._add_date_columns(df, period)
        df = self._add_value_column(df, value_column)
        df = self._add_poa_column(df, poa_column)
        df = self._add_compliance_column(df, value_column, poa_column)
        df = self._order_by_rep_and_product(df)
        df = self._rename_columns(df)
        df = self._select_output_columns(df)
        df = self._clean_data(df, value_column, poa_column)
        df.attrs["column_types"] = self.COLUMN_TYPES
        return df

    def _get_value_column_name(self, period: datetime) -> str:
        month_name = SPANISH_MONTHS.get(period.month, period.strftime("%b"))
        year = period.year
        return f"{self._value_column_base} {month_name} {year}"

    @staticmethod
    def _get_poa_column_name(period: datetime) -> str:
        month_name = SPANISH_MONTHS.get(period.month, period.strftime("%b"))
        year = period.year
        return f"POA {month_name} {year}"

    @staticmethod
    def _add_date_columns(df: pd.DataFrame, period: datetime) -> pd.DataFrame:
        df = df.copy()
        df["date"] = period.strftime("%Y-%m-%d")
        df["transaction_id"] = (
            df["date"].astype(str) + "_" +
            df["rep_id"].astype(str) + "_" +
            df["producto"].astype(str)
        )
        return df

    @staticmethod
    def _add_value_column(df: pd.DataFrame, value_column: str) -> pd.DataFrame:
        df[value_column] = df["valor"]
        return df

    @staticmethod
    def _add_poa_column(df: pd.DataFrame, poa_column: str) -> pd.DataFrame:
        if "poa" in df.columns:
            df[poa_column] = df["poa"]
        else:
            df[poa_column] = None
        return df

    def _add_compliance_column(
        self,
        df: pd.DataFrame,
        value_column: str,
        poa_column: str
    ) -> pd.DataFrame:
        real_values = pd.to_numeric(df[value_column], errors="coerce").fillna(0)
        poa_values = pd.to_numeric(df[poa_column], errors="coerce").fillna(0)

        df["_compliance_raw"] = real_values / poa_values.replace(0, pd.NA)

        if self._bonus_lookup and self._bonus_lookup.is_available():
            bonuses = []
            comisions = []
            for _, row in df.iterrows():
                rep_id = row.get("rep_id", "")
                product = row.get("producto", "")
                compliance = row.get("_compliance_raw")
                bono, comision = self._bonus_lookup.lookup(rep_id, product, compliance)
                bonuses.append(bono)
                comisions.append(comision)
            df["Bono"] = bonuses
            df["Comision"] = comisions
        else:
            df["Bono"] = 0
            df["Comision"] = 0

        df["Cumplimiento %"] = df["_compliance_raw"]
        df = df.drop(columns=["_compliance_raw"])

        return df

    def _order_by_rep_and_product(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'producto' not in df.columns:
            return df

        df['_display_name'] = df['producto'].apply(self._get_display_name)
        order_map = {name: i for i, name in enumerate(self.PRODUCT_ORDER)}
        df['_product_order'] = df['_display_name'].map(order_map).fillna(999)
        df = df.sort_values(['rep_id', '_product_order']).drop(columns=['_product_order', '_display_name'])

        return df.reset_index(drop=True)

    def _get_display_name(self, product: str) -> str:
        return self.PRODUCT_DISPLAY_NAMES.get(product, product)

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'producto_label' in df.columns:
            df['producto_label'] = df['producto_label'].apply(self._transform_product_label)

        rename_dict = {
            "rep_id": "Rep ID",
            "transaction_id": "ID Transaccion",
            "date": "Fecha",
            "producto_label": self._product_column,
        }
        existing_renames = {k: v for k, v in rename_dict.items() if k in df.columns}
        return df.rename(columns=existing_renames)

    def _transform_product_label(self, label: str) -> str:
        if not isinstance(label, str):
            return str(label)
        for old_name, new_name in self.PRODUCT_DISPLAY_NAMES.items():
            label = label.replace(old_name, new_name)
        return label

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = getattr(self, '_current_output_columns', self.OUTPUT_COLUMNS)
        existing = [c for c in columns if c in df.columns]
        return df[existing]

    def _clean_data(self, df: pd.DataFrame, value_column: str, poa_column: str = None) -> pd.DataFrame:
        required = ["Fecha", "Rep ID", "ID Transaccion"]
        for col in required:
            if col in df.columns:
                df = df[df[col].notna() & (df[col] != "") & (df[col] != "nan")]

        if value_column in df.columns:
            if self._metric_type == METRIC_TYPE_VOLUMEN:
                df[value_column] = pd.to_numeric(df[value_column], errors="coerce").fillna(0).astype(int)
            else:
                df[value_column] = pd.to_numeric(df[value_column], errors="coerce").round(2)
                df[value_column] = df[value_column].apply(lambda x: "" if pd.isna(x) else x)

        if poa_column and poa_column in df.columns:
            if self._metric_type == METRIC_TYPE_VOLUMEN:
                df[poa_column] = pd.to_numeric(df[poa_column], errors="coerce").fillna(0).astype(int)
            else:
                df[poa_column] = pd.to_numeric(df[poa_column], errors="coerce").round(2)
                df[poa_column] = df[poa_column].apply(lambda x: "" if pd.isna(x) else x)

        if "Cumplimiento %" in df.columns and len(df) > 0:
            df["Cumplimiento %"] = pd.to_numeric(df["Cumplimiento %"], errors="coerce")
            df["Cumplimiento %"] = df["Cumplimiento %"].apply(
                lambda x: f"{x * 100:.1f}%" if pd.notna(x) else ""
            )

        if "Bono" in df.columns and len(df) > 0:
            def format_bono(x):
                if x == "-":
                    return "-"
                try:
                    val = int(float(x))
                    return val
                except (ValueError, TypeError):
                    return 0
            df["Bono"] = df["Bono"].apply(format_bono)

        if "Comision" in df.columns and len(df) > 0:
            df["Comision"] = pd.to_numeric(df["Comision"], errors="coerce").fillna(0).astype(int)

        return df.fillna("")
