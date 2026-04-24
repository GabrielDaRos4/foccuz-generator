import logging

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .commission_lookup import CommissionLookup

logger = logging.getLogger(__name__)

VALID_BRANDS = ["HONDA", "ACURA"]
DATA_START_DATE = "2025-01-01"
MIN_VEHICLE_YEAR = 2024
TIER_3_MIN_CARS = 5
TIER_2_MIN_CARS = 3
TIER_3_NAME = "Tabla 3"
TIER_2_NAME = "Tabla 2"
TIER_1_NAME = "Tabla 1"
MISSING_COMMISSION_MESSAGE = "Falta Registrar Comision de Modelo"


class MonederoCommissionStrategy(ProcessingStrategy):

    def __init__(self, target_period: str = None):
        self._target_period = target_period
        self._commission_lookup = CommissionLookup()

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        df = data.copy()
        df.columns = df.columns.str.strip()

        df = self._filter_brands(df)
        if df.empty:
            logger.warning("No HONDA or ACURA sales found")
            return pd.DataFrame()

        df = self._filter_by_delivery_date(df)
        if df.empty:
            logger.warning("No sales in target period")
            return pd.DataFrame()

        df = self._filter_valid_rep_ids(df)
        if df.empty:
            logger.warning("No valid Rep IDs found")
            return pd.DataFrame()

        df = self._add_delivery_month(df)
        df = self._add_cars_delivered_count(df)
        df = self._add_commission_type(df)
        df = self._add_car_model(df)
        df = self._calculate_commissions(df)

        result = self._format_output(df)

        missing_models = self._commission_lookup.get_missing_models()
        if missing_models:
            logger.info(f"Models without commission: {len(missing_models)}")
            result.attrs["missing_models"] = missing_models

        return result

    @staticmethod
    def _filter_brands(df: pd.DataFrame) -> pd.DataFrame:
        if "Brand" not in df.columns:
            return df
        return df[df["Brand"].str.upper().isin(VALID_BRANDS)].copy()

    @staticmethod
    def _filter_by_delivery_date(df: pd.DataFrame) -> pd.DataFrame:
        if "Delivery_Date" not in df.columns:
            return df

        result = df.copy()
        result["Delivery_Date"] = pd.to_datetime(result["Delivery_Date"], errors="coerce")
        result = result[result["Delivery_Date"] >= DATA_START_DATE]

        if "Year" in result.columns:
            result["Year"] = pd.to_numeric(result["Year"], errors="coerce")
            result = result[result["Year"] >= MIN_VEHICLE_YEAR]

        return result

    def _filter_valid_rep_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_col = self._find_rep_id_column(df)
        if not rep_col:
            return df

        df[rep_col] = df[rep_col].apply(
            lambda x: None if pd.isna(x) or str(x).strip() in ["", " "] else str(x).strip()
        )
        return df[df[rep_col].notna()].copy()

    @staticmethod
    def _find_rep_id_column(df: pd.DataFrame) -> str | None:
        candidates = ["Rep ID", "rep_id", "IdConsultant", "Consultant_Mail"]
        for col in candidates:
            if col in df.columns:
                return col
        return None

    @staticmethod
    def _add_delivery_month(df: pd.DataFrame) -> pd.DataFrame:
        df["Delivery_Month"] = df["Delivery_Date"].dt.strftime("%Y-%m")
        return df

    def _add_cars_delivered_count(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_col = self._find_rep_id_column(df)
        if not rep_col:
            df["cars_delivered"] = 1
            return df

        deliveries = df.groupby(["Delivery_Month", rep_col]).agg(
            {"Id": "count"}
        ).reset_index()
        deliveries.rename(columns={"Id": "cars_delivered"}, inplace=True)

        df = df.merge(
            deliveries,
            on=["Delivery_Month", rep_col],
            how="left"
        )
        return df

    @staticmethod
    def _add_commission_type(df: pd.DataFrame) -> pd.DataFrame:
        def get_type(cars: int) -> str:
            if cars >= TIER_3_MIN_CARS:
                return TIER_3_NAME
            elif cars >= TIER_2_MIN_CARS:
                return TIER_2_NAME
            return TIER_1_NAME

        df["Tipo de Comision"] = df["cars_delivered"].apply(get_type)
        return df

    @staticmethod
    def _add_car_model(df: pd.DataFrame) -> pd.DataFrame:
        if "Model" not in df.columns:
            df["Car Model"] = ""
            return df

        version_col = "Version" if "Version" in df.columns else None
        year_col = "Year" if "Year" in df.columns else None

        def build_model(row):
            model = str(row["Model"]).strip()
            if version_col and pd.notna(row.get(version_col)):
                model = f"{model} {str(row[version_col]).strip()}"
            if year_col and pd.notna(row.get(year_col)):
                year = int(float(row[year_col]))
                model = f"{model} {year}"
            return model

        df["Car Model"] = df.apply(build_model, axis=1)
        return df

    def _calculate_commissions(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Order_Date" not in df.columns:
            df["Order_Date"] = df["Delivery_Date"]
        else:
            df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")

        def calc_commission(row):
            return self._commission_lookup.lookup(
                brand=row["Brand"],
                car_model=row["Car Model"],
                order_date=row["Order_Date"],
                commission_type=row["Tipo de Comision"]
            )

        df["Comision"] = df.apply(calc_commission, axis=1)

        df["Tipo de Comision"] = df.apply(
            lambda row: MISSING_COMMISSION_MESSAGE
            if pd.isna(row["Comision"]) else row["Tipo de Comision"],
            axis=1
        )
        df["Comision"] = df["Comision"].fillna(0)

        return df

    def _format_output(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_col = self._find_rep_id_column(df)

        output_cols = [
            "Id", "IdOrder", "Status", "idAgency", "Invoice",
            "Delivery_Date", "VIN", "Brand", "Year", "Model", "Version",
            "Customer_Name"
        ]

        if rep_col:
            output_cols.append(rep_col)

        output_cols.extend(["Delivery_Month", "Tipo de Comision", "Comision"])

        existing_cols = [c for c in output_cols if c in df.columns]
        result = df[existing_cols].copy()

        if "Id" in result.columns:
            result.rename(columns={"Id": "ID Transaccion"}, inplace=True)
        if "Delivery_Month" in result.columns:
            result.rename(columns={"Delivery_Month": "Fecha"}, inplace=True)
        if rep_col and rep_col in result.columns:
            result.rename(columns={rep_col: "Rep ID"}, inplace=True)

        if "Fecha" in result.columns:
            result["Fecha"] = pd.to_datetime(result["Fecha"] + "-01")

        result = result.sort_values(by=["Fecha", "Rep ID"] if "Rep ID" in result.columns else ["Fecha"])

        result.attrs["column_types"] = {
            "Fecha": "date",
            "Rep ID": "text",
            "ID Transaccion": "text",
            "Comision": "money",
        }

        return result
