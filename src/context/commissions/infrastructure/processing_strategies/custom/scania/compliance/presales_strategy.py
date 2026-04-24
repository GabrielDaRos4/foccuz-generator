import logging
import math

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class PresalesStrategy(BaseScaniaStrategy):

    COLUMN_RENAME_MAP = {
        "aplica": "Aplica",
        "factura": "Factura",
        "fecha": "Fecha Venta",
        "cliente": "Cliente",
        "rut cliente": "Rut Cliente",
        "facturado a": "Facturado A",
        "modelo": "Modelo",
        "chasis": "Chasis",
        "tc": "TC",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Aplica", "Factura", "Fecha Venta", "Cliente", "Rut Cliente",
        "Facturado A", "Modelo", "Chasis", "TC", "Comisión"
    ]

    COLUMN_TYPES = {
        "TC": "decimal",
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

        configurations_data = self._get_configurations_data(result)
        if configurations_data.empty:
            logger.warning("No configurations data found in Estudio_Configuraciones")
            result["commission"] = 0
            return result

        result = self._merge_configurations_with_employees(result, configurations_data)
        result = self._calculate_commission(result)

        return result

    def _get_configurations_data(self, df: pd.DataFrame) -> pd.DataFrame:
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        for key, secondary_df in secondary_arrays.items():
            if "estudio" in key.lower() or "configuracion" in key.lower():
                logger.info(f"Found configurations data in secondary array '{key}': {len(secondary_df)} records")
                return secondary_df.copy()

        for col in df.columns:
            if "chasis" in col.lower() and "tc" in [c.lower() for c in df.columns]:
                logger.info("Configurations data already merged with employees")
                return df.copy()

        return pd.DataFrame()

    def _merge_configurations_with_employees(
        self, employees_df: pd.DataFrame, configs_df: pd.DataFrame
    ) -> pd.DataFrame:
        if "chasis" in [c.lower() for c in employees_df.columns]:
            return employees_df

        configs_df = configs_df.copy()
        configs_df.columns = configs_df.columns.str.lower().str.strip()

        employees_df = employees_df.copy()

        employees_df['_rut_clean'] = self._normalize_rut(employees_df['rut'])

        rut_config_col = next(
            (c for c in configs_df.columns if 'rut' in c and 'config' in c), None
        )
        if not rut_config_col:
            rut_config_col = next((c for c in configs_df.columns if 'rut' in c), None)

        if rut_config_col:
            configs_df['_rut_clean'] = self._normalize_rut(configs_df[rut_config_col])
        else:
            logger.warning("No RUT column found in configurations data")
            return employees_df

        result = configs_df.merge(
            employees_df[['_rut_clean', 'id empleado', 'rut', 'nombre completo']],
            on='_rut_clean',
            how='inner'
        )

        result = result.drop(columns=['_rut_clean'], errors='ignore')

        logger.info(
            f"Merged {len(employees_df)} employees with "
            f"{len(configs_df)} configurations: {len(result)} records"
        )
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

        aplica_col = self._find_column(result, 'aplica')
        tc_col = self._find_column(result, 'tc')

        if not aplica_col or not tc_col:
            logger.warning("Missing 'aplica' or 'tc' column")
            result["commission"] = 0
            return result

        aplica = result[aplica_col].astype(str).str.strip().str.lower() == "si"
        tc = pd.to_numeric(result[tc_col], errors='coerce').fillna(0)

        result["commission"] = 0
        result.loc[aplica, "commission"] = (tc[aplica] * 100).apply(math.ceil)

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

        if chasis_col:
            result["ID Transacción"] = (
                result["Fecha"].astype(str) + "_" + result[chasis_col].fillna("").astype(str)
            )
        else:
            result["ID Transacción"] = (
                result["Fecha"].astype(str) + "_" + result.index.astype(str)
            )

        return result


PreventaStrategy = PresalesStrategy
