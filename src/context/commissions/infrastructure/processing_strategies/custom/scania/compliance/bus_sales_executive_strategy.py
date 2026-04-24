import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class BusSalesExecutiveStrategy(BaseScaniaStrategy):

    COMMISSION_BY_CLIENT_TYPE = {
        "flotista": 300,
        "nuevo": 780,
        "corporativo": 100,
    }

    COLUMN_RENAME_MAP = {
        "comision vendedor": "Comision Vendedor",
        "jefatura": "Jefatura",
        "factura": "Factura",
        "cliente": "Cliente",
        "rut cliente": "Rut Cliente",
        "facturado a": "Facturado A",
        "tipo cliente": "Tipo Cliente",
        "modelo": "Modelo",
        "chasis": "Chasis",
        "valor usd": "Valor USD",
        "tc": "TC",
        "valor $": "Valor $",
        "commission_type_usd": "Tipo De Comisión",
        "commission_type_payment": "Pago Tipo De Comisión",
        "cliente_nuevo": "Cliente Nuevo",
        "clientes_flotista": "Clientes Flotista",
        "clientes_corporativo": "Clientes Corporativo",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Sucursal", "Comision Vendedor", "Jefatura",
        "Factura", "Cliente", "Rut Cliente", "Facturado A",
        "Tipo Cliente", "Modelo", "Chasis",
        "Valor USD", "TC", "Valor $",
        "Tipo De Comisión", "Pago Tipo De Comisión",
        "Cliente Nuevo", "Clientes Flotista", "Clientes Corporativo",
        "Comisión"
    ]

    COLUMN_TYPES = {
        "Valor USD": "money",
        "TC": "decimal",
        "Valor $": "money",
        "Tipo De Comisión": "money",
        "Pago Tipo De Comisión": "money",
        "Cliente Nuevo": "money",
        "Clientes Flotista": "money",
        "Clientes Corporativo": "money",
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
        result = self._calculate_commission_by_client_type(result)

        return result

    def _get_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        for key, secondary_df in secondary_arrays.items():
            if "venta" in key.lower():
                logger.info(f"Found sales data in secondary array '{key}': {len(secondary_df)} records")
                return secondary_df.copy()

        if all(col in df.columns for col in ['chasis', 'tc']):
            logger.info("Sales data already merged with employees")
            return df.copy()

        return pd.DataFrame()

    def _merge_sales_with_employees(
        self, employees_df: pd.DataFrame, sales_df: pd.DataFrame
    ) -> pd.DataFrame:
        if all(col in employees_df.columns for col in ['chasis', 'tc']):
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
            has_hyphen = "-" in original
            cleaned = original.replace(".", "").replace("-", "")
            if has_hyphen and len(cleaned) >= 8:
                return cleaned[:-1]
            return cleaned
        return series.apply(clean_rut)

    def _calculate_commission_by_client_type(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        tipo_cliente_col = self._find_column(result, 'tipo cliente', 'tipo_cliente')
        comision_vendedor_col = self._find_column(result, 'comision vendedor', 'comision_vendedor')
        tc_col = self._find_column(result, 'tc')

        if not tipo_cliente_col or not tc_col:
            logger.warning(f"Missing required columns: tipo_cliente={tipo_cliente_col}, tc={tc_col}")
            result["commission"] = 0
            return result

        tipo_cliente = result[tipo_cliente_col].astype(str).str.strip().str.lower()
        tc = pd.to_numeric(result[tc_col], errors='coerce').fillna(0)

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

        result["commission_type_usd"] = tipo_cliente.map(self.COMMISSION_BY_CLIENT_TYPE).fillna(0)
        result["commission_type_payment"] = result["commission_type_usd"] * tc

        result["cliente_nuevo"] = 0.0
        result["clientes_flotista"] = 0.0
        result["clientes_corporativo"] = 0.0

        nuevo_mask = has_commission & (tipo_cliente == "nuevo")
        result.loc[nuevo_mask, "cliente_nuevo"] = result.loc[nuevo_mask, "commission_type_payment"]

        flotista_mask = has_commission & (tipo_cliente == "flotista")
        result.loc[flotista_mask, "clientes_flotista"] = result.loc[flotista_mask, "commission_type_payment"]

        corporativo_mask = has_commission & (tipo_cliente == "corporativo")
        result.loc[corporativo_mask, "clientes_corporativo"] = result.loc[corporativo_mask, "commission_type_payment"]

        result["commission"] = (
            result["cliente_nuevo"] +
            result["clientes_flotista"] +
            result["clientes_corporativo"]
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


EjecutivoVentaBusesStrategy = BusSalesExecutiveStrategy
