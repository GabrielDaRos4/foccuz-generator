import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class NewSalesExecutiveStrategy(BaseScaniaStrategy):

    BASE_COMMISSION_RATE = 0.0075
    ADDITIONAL_COMMISSION_RATE = 0.0025
    MAX_DAYS_IN_STOCK_FOR_BONUS = 30

    COLUMN_RENAME_MAP = {
        "sucursal": "Sucursal",
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
        "fecha venta": "Fecha Venta",
        "cliente": "Cliente",
        "tipo cliente": "Tipo Cliente",
        "participacion": "Participacion",
        "rut cliente": "Rut Cliente",
        "facturado a": "Facturado A",
        "modelo": "Modelo",
        "chasis": "Chasis",
        "valor usd": "Valor USD",
        "tc": "TC",
        "valor $": "Valor $",
        "comision_nuevo": "Comision Nuevo",
        "comision_adicional_nuevo": "Comision Adicional Nuevo",
        "adicional_corporativo": "Adicional Corporativo",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Sucursal", "Comision Vendedor", "Jefatura", "Coordinacion",
        "En Sistema", "Fecha Reconc", "Reconocimiento", "Operaciones",
        "Fecha Arribo", "Dias En Stock", "Jefe De Ventas", "Factura",
        "Fecha Venta", "Cliente", "Tipo Cliente", "Participacion",
        "Rut Cliente", "Facturado A", "Modelo", "Chasis",
        "Valor USD", "TC", "Valor $",
        "Comision Nuevo", "Comision Adicional Nuevo", "Adicional Corporativo",
        "Comisión"
    ]

    COLUMN_TYPES = {
        "Valor USD": "money",
        "TC": "decimal",
        "Valor $": "money",
        "Comision Nuevo": "money",
        "Comision Adicional Nuevo": "money",
        "Adicional Corporativo": "money",
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
        result = self._rename_fecha_column(result)
        result = self._calculate_commissions(result)

        return result

    def _get_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        for key, secondary_df in secondary_arrays.items():
            if "venta" in key.lower():
                logger.info(f"Found sales data in secondary array '{key}': {len(secondary_df)} records")
                return secondary_df.copy()

        if all(col in df.columns for col in ['chasis', 'valor $']):
            logger.info("Sales data already merged with employees")
            return df.copy()

        return pd.DataFrame()

    def _merge_sales_with_employees(
        self, employees_df: pd.DataFrame, sales_df: pd.DataFrame
    ) -> pd.DataFrame:
        if all(col in employees_df.columns for col in ['chasis', 'valor $']):
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

    def _rename_fecha_column(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        fecha_col = next((c for c in result.columns if c.lower() == 'fecha'), None)
        if fecha_col and 'fecha venta' not in result.columns:
            result = result.rename(columns={fecha_col: 'fecha venta'})
        return result

    def _calculate_commissions(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        valor_col = self._find_column(result, 'valor $', 'valor_$')
        comision_vendedor_col = self._find_column(result, 'comision vendedor', 'comision_vendedor')
        dias_stock_col = self._find_column(result, 'dias en stock', 'dias_en_stock')

        if not valor_col:
            logger.warning("Missing 'valor $' column")
            result["commission"] = 0
            return result

        valor = pd.to_numeric(result[valor_col], errors='coerce').fillna(0)

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

        result["comision_nuevo"] = 0.0
        result.loc[has_commission, "comision_nuevo"] = valor[has_commission] * self.BASE_COMMISSION_RATE

        result["comision_adicional_nuevo"] = 0.0
        if dias_stock_col:
            dias_stock = pd.to_numeric(result[dias_stock_col], errors='coerce').fillna(999)
            bonus_eligible = has_commission & (dias_stock <= self.MAX_DAYS_IN_STOCK_FOR_BONUS)
            result.loc[bonus_eligible, "comision_adicional_nuevo"] = (
                valor[bonus_eligible] * self.ADDITIONAL_COMMISSION_RATE
            )

        result["adicional_corporativo"] = 0

        result["commission"] = (
            result["comision_nuevo"] +
            result["comision_adicional_nuevo"] +
            result["adicional_corporativo"]
        ).fillna(0).astype(int)

        return result

    def _find_column(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None

    def _create_transaction_id(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        chasis_col = self._find_column(result, 'chasis')

        if chasis_col and "Rep ID" in result.columns:
            result["ID Transacción"] = (
                result[chasis_col].astype(str) + "_" + result["Rep ID"].astype(str)
            )
        elif "Rep ID" in result.columns:
            result["ID Transacción"] = (
                result["Fecha"].astype(str) + "_" + result["Rep ID"].astype(str)
            )
        else:
            result["ID Transacción"] = result["Fecha"].astype(str) + "_" + result.index.astype(str)

        return result


EjecutivoVentaNuevoStrategy = NewSalesExecutiveStrategy
