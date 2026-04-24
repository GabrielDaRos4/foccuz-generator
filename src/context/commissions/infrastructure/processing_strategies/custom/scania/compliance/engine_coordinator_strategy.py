import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class EngineCoordinatorStrategy(BaseScaniaStrategy):

    COLUMN_RENAME_MAP = {
        "sucursal": "Sucursal",
        "producto": "Producto",
        "comision vendedor": "Comision Vendedor",
        "jefatura": "Jefatura",
        "coordinacion": "Coordinacion",
        "en sistema": "En Sistema",
        "fecha reconc": "Fecha Reconc",
        "reconocimiento": "Reconocimiento",
        "operaciones": "Operaciones",
        "fecha arribo": "Fecha Arribo",
        "dias en stock": "Dias En Stock",
        "jefe de ventas": "Jefe De Ventas",
        "factura": "Factura",
        "fecha factura": "Fecha Factura",
        "cliente": "Cliente",
        "rut cliente": "Rut Cliente",
        "facturado a": "Facturado A",
        "modelo": "Modelo",
        "chasis": "Chasis",
        "valor usd": "Valor USD",
        "tc": "TC",
        "valor $": "Valor $",
        "comision_pct": "Porcentaje Comision",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Sucursal", "Producto", "Comision Vendedor", "Jefatura", "Coordinacion",
        "En Sistema", "Fecha Reconc", "Reconocimiento", "Operaciones",
        "Fecha Arribo", "Dias En Stock", "Jefe De Ventas",
        "Factura", "Fecha Factura", "Cliente", "Rut Cliente", "Facturado A",
        "Modelo", "Chasis", "Valor USD", "TC", "Valor $",
        "Porcentaje Comision", "Comisión"
    ]

    COLUMN_TYPES = {
        "Valor USD": "money",
        "TC": "decimal",
        "Valor $": "money",
        "Porcentaje Comision": "percentage",
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

        if all(col in [c.lower() for c in df.columns] for col in ['chasis', 'valor $']):
            logger.info("Sales data already merged with employees")
            return df.copy()

        return pd.DataFrame()

    def _merge_sales_with_employees(
        self, employees_df: pd.DataFrame, sales_df: pd.DataFrame
    ) -> pd.DataFrame:
        if 'chasis' in [c.lower() for c in employees_df.columns]:
            return employees_df

        sales_df = sales_df.copy()
        sales_df.columns = sales_df.columns.str.lower().str.strip()

        employees_df = employees_df.copy()

        employees_df['_rut_clean'] = self._normalize_rut(employees_df['rut'])

        rut_col = next((c for c in sales_df.columns if c == 'rut'), None)
        if rut_col:
            sales_df['_rut_clean'] = self._normalize_rut(sales_df[rut_col])
        else:
            logger.warning("No RUT column found in sales data")
            return employees_df

        result = sales_df.merge(
            employees_df[['_rut_clean', 'id empleado', 'rut', 'nombre completo']],
            on='_rut_clean',
            how='inner'
        )

        result = result.drop(columns=['_rut_clean'], errors='ignore')

        logger.info(f"Merged {len(employees_df)} employees with {len(sales_df)} sales: {len(result)} records")
        return result

    def _normalize_rut(self, series: pd.Series) -> pd.Series:
        def clean_rut(rut: str) -> str:
            original = str(rut).strip()
            if original.upper() == "N/A" or original.lower() == "nan":
                return ""
            cleaned = original.replace(".", "").replace("-", "")
            cleaned = ''.join(filter(str.isdigit, cleaned))
            if len(cleaned) >= 8:
                return cleaned[:-1]
            return cleaned
        return series.apply(clean_rut)

    def _calculate_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        comision_vendedor_col = self._find_column(result, 'comision vendedor')
        valor_col = self._find_column(result, 'valor $', 'valor_$')
        comision_pct_col = self._find_exact_column(result, 'comision')

        if not valor_col:
            logger.warning("Missing 'valor $' column")
            result["commission"] = 0
            return result

        valor = pd.to_numeric(result[valor_col], errors='coerce').fillna(0)

        if comision_pct_col:
            comision_pct = pd.to_numeric(result[comision_pct_col], errors='coerce').fillna(0)
            if comision_pct.max() > 1:
                comision_pct = comision_pct / 100
            result["comision_pct"] = comision_pct
        else:
            result["comision_pct"] = 0.01

        if comision_vendedor_col:
            has_commission = (
                result[comision_vendedor_col]
                .astype(str)
                .str.strip()
                .str.upper()
                == "SI"
            )
        else:
            has_commission = pd.Series([True] * len(result), index=result.index)

        result["commission"] = 0.0
        result.loc[has_commission, "commission"] = (
            valor[has_commission] * result.loc[has_commission, "comision_pct"]
        ).fillna(0).astype(int)

        return result

    def _find_exact_column(self, df: pd.DataFrame, column_name: str) -> str | None:
        for col in df.columns:
            if col.lower().strip() == column_name.lower().strip():
                return col
        return None

    def _find_column(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None

    def _create_transaction_id(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        chasis_col = self._find_column(result, 'chasis')

        if chasis_col:
            result["ID Transacción"] = (
                result["Fecha"].astype(str) + "_" + result[chasis_col].fillna("").astype(str)
            )
        else:
            result["ID Transacción"] = (
                result["Fecha"].astype(str) + "_" + result.index.astype(str)
            )

        return result


CoordinadorMotoresStrategy = EngineCoordinatorStrategy
