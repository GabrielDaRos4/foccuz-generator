import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class UsedSalesExecutiveStrategy(BaseScaniaStrategy):

    COMMISSION_RATE = 0.01

    COLUMN_RENAME_MAP = {
        "vendedor": "Vendedor",
        "retomado": "Retomado",
        "cliente": "Cliente",
        "rut cliente": "Rut Cliente",
        "forma de pago": "Forma De Pago",
        "patente": "Patente",
        "tipo": "Tipo",
        "marca": "Marca",
        "modelo": "Modelo",
        "year": "Year",
        "fecha venta": "Fecha Venta",
        "tipo venta": "Tipo Venta",
        "precio lista": "Precio Lista",
        "precio venta": "Precio Venta",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Vendedor", "Retomado", "Cliente", "Rut Cliente",
        "Forma De Pago", "Patente", "Tipo", "Marca", "Modelo", "Year",
        "Fecha Venta", "Tipo Venta", "Precio Lista", "Precio Venta",
        "Comisión"
    ]

    COLUMN_TYPES = {
        "Precio Lista": "money",
        "Precio Venta": "money",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': self._role_filter,
            'filtered_out_by_role': 0,
        }

        result = df.copy()
        result.attrs = df.attrs.copy()

        diagnostics['matched_rows'] = len(result)
        return result, diagnostics

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        sales_data = self._get_sales_data(result)
        if sales_data.empty:
            logger.warning("No sales data found in Venta_Neta")
            result["commission"] = 0
            return result

        result = self._merge_sales_with_employees(result, sales_data)
        result = self._calculate_commission(result)

        return result

    def _get_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        for key, secondary_df in secondary_arrays.items():
            if "venta" in key.lower():
                logger.info(f"Found sales data in secondary array '{key}': {len(secondary_df)} records")
                return secondary_df.copy()

        if all(col in df.columns for col in ['patente', 'precio venta']):
            logger.info("Sales data already merged with employees")
            return df.copy()

        return pd.DataFrame()

    def _merge_sales_with_employees(
        self, employees_df: pd.DataFrame, sales_df: pd.DataFrame
    ) -> pd.DataFrame:
        if all(col in employees_df.columns for col in ['patente', 'precio venta']):
            return employees_df

        sales_df = sales_df.copy()
        sales_df.columns = sales_df.columns.str.lower().str.strip()

        employees_df = employees_df.copy()

        employees_df['_rut_clean'] = self._normalize_rut(employees_df['rut'])
        sales_df['_rut_clean'] = self._normalize_rut(sales_df['rut'])

        result = employees_df.merge(
            sales_df,
            on='_rut_clean',
            how='inner',
            suffixes=('', '_sale')
        )

        result = result.drop(columns=['_rut_clean'], errors='ignore')

        logger.info(f"Merged {len(employees_df)} employees with {len(sales_df)} sales: {len(result)} records")
        return result

    def _normalize_rut(self, series: pd.Series) -> pd.Series:
        def clean_rut(rut: str) -> str:
            original = str(rut).strip()
            if original.upper() == "N/A" or original.lower() == "nan":
                return ""
            has_hyphen = "-" in original
            cleaned = original.replace(".", "").replace("-", "")
            if has_hyphen and len(cleaned) >= 8:
                return cleaned[:-1]
            return cleaned
        return series.apply(clean_rut)

    def _calculate_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        precio_venta_col = self._find_column(result, 'precio venta', 'precio_venta')

        if not precio_venta_col:
            logger.warning("Missing 'precio venta' column")
            result["commission"] = 0
            return result

        precio_venta = pd.to_numeric(result[precio_venta_col], errors='coerce').fillna(0)

        result["commission"] = (precio_venta * self.COMMISSION_RATE).astype(int)

        return result

    def _find_column(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None

    def _create_transaction_id(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        patente_col = self._find_column(result, 'patente')

        if patente_col and "Rep ID" in result.columns:
            result["ID Transacción"] = (
                result[patente_col].astype(str) + "_" + result["Rep ID"].astype(str)
            )
        elif "Rep ID" in result.columns:
            result["ID Transacción"] = (
                result["Fecha"].astype(str) + "_" + result["Rep ID"].astype(str)
            )
        else:
            result["ID Transacción"] = result["Fecha"].astype(str) + "_" + result.index.astype(str)

        return result


EjecutivoVentaUsadoStrategy = UsedSalesExecutiveStrategy
