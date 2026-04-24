import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .output_formatter import GocarOutputFormatter
from .rep_matcher import RepMatcher

logger = logging.getLogger(__name__)


COLUMN_MAPPINGS = {
    'agente o vendedor': 'agente',
    'u. bruta': 'utilidad_bruta',
    '% comision': 'pct_comision',
    'f. factura': 'fecha_factura',
    'bancos\nfinanciamientos': 'financiamiento',
    'bonos/ otros': 'bonos_otros',
    'comision ingreso total': 'comision_ingreso_total',
    'acumulado de comisiones': 'acumulado_comisiones',
    'descuentos 2': 'descuentos_2',
    'fecha de pago': 'fecha_pago',
}

NUMERIC_COLUMNS = [
    'utilidad_bruta', 'pct_comision', 'comision', 'toma', 'financiamiento',
    'edegas', 'verificación', 'accesorios', 'garantias', 'seguros',
    'placas', 'bonos_otros', 'comision_ingreso_total', 'descuentos',
    'acumulado_comisiones', 'acumulado', 'descuentos_2',
]


class GocarCommissionStrategy(ProcessingStrategy):
    MIN_VALID_YEAR = 2025

    def __init__(
        self,
        department_type: str,
        target_period: str = None,
        rep_id_filter: str = None
    ):
        self._department_type = department_type.upper()
        self._target_period = target_period
        self._rep_id_filter = rep_id_filter
        self._output_formatter = GocarOutputFormatter()

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return pd.DataFrame()

        users_mapping = data.attrs.get('users_mapping')
        if users_mapping is None:
            logger.warning("Users mapping not found in data attributes")
            return pd.DataFrame()

        rep_matcher = RepMatcher.from_dataframe(users_mapping)

        data = self._normalize_columns(data)
        data = self._map_columns(data)
        data = self._clean_numeric_columns(data)
        data = self._clean_date_columns(data)

        data = self._filter_by_department(data)
        if data.empty:
            logger.warning(f"No data found for department: {self._department_type}")
            return pd.DataFrame()

        data = self._filter_valid_rows(data)
        if data.empty:
            return pd.DataFrame()

        data = self._assign_rep_ids(data, rep_matcher)

        data = self._filter_rows_with_rep_id(data)
        if data.empty:
            logger.warning("No rows with valid Rep ID found")
            return pd.DataFrame()

        if self._rep_id_filter:
            data = self._filter_by_rep_id(data)
            if data.empty:
                return pd.DataFrame()

        result = self._build_output(data)

        return self._output_formatter.format(result)

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()
        return df

    @staticmethod
    def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
        rename_dict = {}
        for original, target in COLUMN_MAPPINGS.items():
            if original in df.columns:
                rename_dict[original] = target
        return df.rename(columns=rename_dict)

    @staticmethod
    def _clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
        for col in NUMERIC_COLUMNS:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace('$', '', regex=False)
                    .str.replace(',', '', regex=False)
                    .str.strip()
                )
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df

    @staticmethod
    def _clean_date_columns(df: pd.DataFrame) -> pd.DataFrame:
        if 'fecha_factura' in df.columns:
            df['fecha_factura'] = pd.to_datetime(df['fecha_factura'], errors='coerce')

        if 'fecha_pago' in df.columns:
            df['fecha_pago'] = pd.to_datetime(df['fecha_pago'], errors='coerce')

        if 'semana' in df.columns:
            df['semana'] = pd.to_numeric(df['semana'], errors='coerce')

        return df

    def _filter_by_department(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'depto' not in df.columns:
            logger.warning("Column 'depto' not found")
            return pd.DataFrame()

        if self._department_type == 'NUEVOS':
            mask = (
                df['depto'].str.contains('NUEVOS', na=False, case=False) &
                ~df['depto'].str.contains('SEMINUEVOS', na=False, case=False)
            )
        elif self._department_type == 'SEMINUEVOS':
            mask = df['depto'].str.contains('SEMINUEVOS', na=False, case=False)
        else:
            mask = df['depto'].str.contains(self._department_type, na=False, case=False)

        filtered = df[mask].copy()
        logger.info(f"Filtered by department '{self._department_type}': {len(filtered)} rows")
        return filtered

    def _filter_valid_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'agente' not in df.columns:
            logger.warning("Column 'agente' not found")
            return pd.DataFrame()

        df = df[df['agente'].notna()].copy()
        df = df[~df['agente'].str.contains('SEMANA', na=False, case=False)].copy()

        if 'fecha_pago' in df.columns:
            df = df[df['fecha_pago'].notna()].copy()
            df = self._filter_by_valid_year(df)

        logger.info(f"Valid rows after filtering: {len(df)}")
        return df

    def _filter_by_valid_year(self, df: pd.DataFrame) -> pd.DataFrame:
        initial_count = len(df)

        mask = df['fecha_pago'].dt.year >= self.MIN_VALID_YEAR
        df = df[mask].copy()

        filtered_count = initial_count - len(df)
        if filtered_count > 0:
            logger.info(f"Filtered out {filtered_count} rows with dates before {self.MIN_VALID_YEAR}")

        return df

    @staticmethod
    def _assign_rep_ids(df: pd.DataFrame, rep_matcher: RepMatcher) -> pd.DataFrame:
        df['rep_id'] = df['agente'].apply(rep_matcher.find_rep_id)

        matched = df['rep_id'].notna().sum()
        unmatched = df['rep_id'].isna().sum()
        logger.info(f"Rep ID assignment: {matched} matched, {unmatched} unmatched")

        if unmatched > 0:
            unmatched_agents = df[df['rep_id'].isna()]['agente'].unique()
            logger.warning(f"Unmatched agents: {list(unmatched_agents[:10])}")

        return df

    @staticmethod
    def _filter_rows_with_rep_id(df: pd.DataFrame) -> pd.DataFrame:
        return df[df['rep_id'].notna()].copy()

    def _filter_by_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_id_str = str(self._rep_id_filter).strip()
        filtered = df[df['rep_id'].astype(str).str.strip() == rep_id_str].copy()
        logger.info(f"Filtered by Rep ID {rep_id_str}: {len(filtered)} records")
        return filtered

    def _build_output(self, df: pd.DataFrame) -> pd.DataFrame:
        records = []
        counter = 0

        for _, row in df.iterrows():
            counter += 1

            utilidad_bruta = float(row.get('utilidad_bruta', 0))
            pct_comision = float(row.get('pct_comision', 0))
            comision_base = utilidad_bruta * pct_comision

            toma = float(row.get('toma', 0))
            financiamiento = float(row.get('financiamiento', 0))
            edegas = float(row.get('edegas', 0))
            verificacion = float(row.get('verificación', 0))
            accesorios = float(row.get('accesorios', 0))
            garantias = float(row.get('garantias', 0))
            seguros = float(row.get('seguros', 0))
            placas = float(row.get('placas', 0))
            bonos_otros = float(row.get('bonos_otros', 0))
            descuentos = float(row.get('descuentos', 0))

            comision_ingreso_total = (
                comision_base + toma + financiamiento + edegas + verificacion +
                accesorios + garantias + seguros + placas + bonos_otros
            )

            comision_final = comision_ingreso_total - descuentos

            record = {
                'Fecha': self._format_date_first_day(row.get('fecha_pago')),
                'Fecha de Pago': self._format_date(row.get('fecha_pago')),
                'Rep ID': int(row['rep_id']),
                'ID Transaccion': self._create_transaction_id(row, counter),
                'Negocio': str(row.get('depto', '')) if pd.notna(row.get('depto')) else '',
                'Cliente': str(row.get('cliente', '')) if pd.notna(row.get('cliente')) else '',
                'Chasis': str(row.get('chasis', '')) if pd.notna(row.get('chasis')) else '',
                'Condiciones': str(row.get('condiciones', '')) if pd.notna(row.get('condiciones')) else '',
                'Factura': str(row.get('factura', '')) if pd.notna(row.get('factura')) else '',
                'Utilidad Bruta': utilidad_bruta,
                '% Comision': pct_comision,
                'Comision Base': comision_base,
                'Toma': toma,
                'Financiamiento': financiamiento,
                'Edegas': edegas,
                'Verificacion': verificacion,
                'Accesorios': accesorios,
                'Garantias': garantias,
                'Seguros': seguros,
                'Placas': placas,
                'Bonos Otros': bonos_otros,
                'Descuentos': descuentos,
                'Semana': int(row.get('semana', 0)) if pd.notna(row.get('semana')) else 0,
                'Comision': comision_final,
            }

            records.append(record)

        return pd.DataFrame(records)

    @staticmethod
    def _format_date(date_value) -> str:
        if pd.isna(date_value):
            return ''

        if isinstance(date_value, (datetime, pd.Timestamp)):
            return date_value.strftime('%Y-%m-%d')

        return str(date_value)

    @staticmethod
    def _format_date_first_day(date_value) -> str:
        if pd.isna(date_value):
            return ''

        if isinstance(date_value, (datetime, pd.Timestamp)):
            first_day_of_month = date_value.replace(day=1)
            return first_day_of_month.strftime('%Y-%m-%d')

        return str(date_value)

    def _create_transaction_id(self, row, counter: int) -> str:
        tipo = self._department_type
        rep_id = row.get('rep_id', 'UNKNOWN')

        chasis = str(row.get('chasis', ''))
        chasis_clean = chasis.replace(' ', '').replace('-', '')[:10] if chasis else 'NOCHASIS'

        fecha = row.get('fecha_pago')
        fecha_str = fecha.strftime('%Y%m%d') if pd.notna(fecha) else 'NOFECHA'

        return f"{tipo}_{rep_id}_{chasis_clean}_{fecha_str}_{counter}"

    def get_column_types(self) -> dict[str, str]:
        return self._output_formatter.COLUMN_TYPES
