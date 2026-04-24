import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

from .output_formatter import PoaComplianceOutputFormatter
from .poa_lookup import PoaLookup
from .product_config import PRODUCT_CONFIGS, ProductConfig

logger = logging.getLogger(__name__)

REP_ID_COL = "rut_ejecutivo"
REP_ID_DV_COL = "dv_ejecutivo"

METRIC_TYPE_VOLUMEN = "volumen"
METRIC_TYPE_MARGEN = "margen"


class CopecPoaComplianceStrategy(ProcessingStrategy):

    def __init__(
        self,
        target_period: str = None,
        rep_id_filter: str = None,
        metric_type: str = METRIC_TYPE_VOLUMEN
    ):
        self._target_period = target_period
        self._rep_id_filter = rep_id_filter
        self._metric_type = metric_type
        self._output_formatter = PoaComplianceOutputFormatter(metric_type)

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        sources = data.attrs.get('sources', {})

        if not sources:
            logger.warning("No sources data found")
            return pd.DataFrame()

        all_rep_ids = self._collect_all_rep_ids(sources)
        if not all_rep_ids:
            logger.warning("No rep_ids found in sources")
            return pd.DataFrame()

        results = []
        for rep_id in all_rep_ids:
            rows = self._calculate_rep_product_rows(sources, rep_id)
            results.extend(rows)

        if not results:
            return pd.DataFrame()

        result_df = pd.DataFrame(results)

        if self._metric_type == METRIC_TYPE_VOLUMEN:
            ce_appce_rows = self._create_combined_rows(result_df, 'CE + AppCE', ['CE', 'AppCE'], 'M3')
            bluemax_total_rows = self._create_combined_rows(
                result_df, 'Bluemax Total', ['Bluemax TCT', 'Bluemax AppCE'], 'M3'
            )
            result_df = pd.concat(
                [result_df, pd.DataFrame(ce_appce_rows), pd.DataFrame(bluemax_total_rows)],
                ignore_index=True
            )

        if self._metric_type == METRIC_TYPE_MARGEN:
            result_df = result_df[result_df['valor'].notna() & (result_df['valor'] != 0)].copy()

        if self._rep_id_filter:
            result_df = self._filter_by_rep_id(result_df)
            if result_df.empty:
                return pd.DataFrame()

        period = self._extract_period()

        result_df = self._add_poa_column(result_df, sources, period)

        bonus_df = sources.get('BONUS')

        return self._output_formatter.format(result_df, period, bonus_df)

    def _add_poa_column(
        self,
        df: pd.DataFrame,
        sources: dict[str, pd.DataFrame],
        period: datetime
    ) -> pd.DataFrame:
        poa_df = sources.get('POA_RESUMEN')

        if poa_df is None or poa_df.empty:
            logger.info("No POA data available, skipping POA column")
            df['poa'] = None
            return df

        poa_lookup = PoaLookup(poa_df, self._metric_type, period)

        if not poa_lookup.is_available():
            logger.warning("POA lookup not available for the target period")
            df['poa'] = None
            return df

        df['poa'] = df.apply(
            lambda row: poa_lookup.lookup(row['rep_id'], row['producto']),
            axis=1
        )

        logger.info(f"Added POA column: {df['poa'].notna().sum()} values found")

        return df

    def _collect_all_rep_ids(self, sources: dict[str, pd.DataFrame]) -> set[str]:
        all_rep_ids = set()

        for source_df in sources.values():
            if source_df is None or source_df.empty:
                continue

            df = self._normalize_columns(source_df)
            rep_ids = self._extract_rep_ids_from_df(df)
            all_rep_ids.update(rep_ids)

        logger.info(f"Found {len(all_rep_ids)} unique rep_ids")
        return all_rep_ids

    @staticmethod
    def _extract_rep_ids_from_df(df: pd.DataFrame) -> set[str]:
        rep_ids = set()

        if REP_ID_COL in df.columns:
            if REP_ID_DV_COL in df.columns:
                for _, row in df.iterrows():
                    rut = str(row[REP_ID_COL]).strip()
                    dv = str(row[REP_ID_DV_COL]).strip().upper()
                    if rut and rut.lower() not in ('nan', 'none', '', 'sin_informacion'):
                        if '-' in rut:
                            rep_ids.add(rut.upper() if rut[-1].isalpha() else rut)
                        else:
                            rep_ids.add(f"{rut}-{dv}")
            else:
                for rut in df[REP_ID_COL].dropna().unique():
                    rut_str = str(rut).strip()
                    if rut_str and rut_str.lower() not in ('nan', 'none', '', 'sin_informacion'):
                        if rut_str[-1].isalpha():
                            rut_str = rut_str[:-1] + rut_str[-1].upper()
                        rep_ids.add(rut_str)

        return rep_ids

    def _calculate_rep_product_rows(
        self,
        sources: dict[str, pd.DataFrame],
        rep_id: str
    ) -> list[dict]:
        rows = []

        for config in PRODUCT_CONFIGS:
            volume, margin = self._calculate_product_metrics_for_rep(sources, config, rep_id)
            converted_volume = volume / config.volume_divisor if config.volume_divisor else volume

            if self._metric_type == METRIC_TYPE_VOLUMEN:
                producto_label = f"{config.name} ({config.volume_unit})"
                valor = converted_volume
            else:
                if config.margin_unit is None:
                    continue
                producto_label = f"{config.name} ({config.margin_unit})"
                valor = margin

            rows.append({
                'rep_id': rep_id,
                'producto': config.name,
                'producto_label': producto_label,
                'valor': valor,
                'volume_unit': config.volume_unit,
            })

        return rows

    @staticmethod
    def _create_combined_rows(
        df: pd.DataFrame,
        combined_name: str,
        source_products: list[str],
        unit: str
    ) -> list[dict]:
        rows = []

        for rep_id in df['rep_id'].unique():
            rep_df = df[df['rep_id'] == rep_id]
            product_rows = rep_df[rep_df['producto'].isin(source_products)]

            total_value = product_rows['valor'].sum() if not product_rows.empty else 0
            rows.append({
                'rep_id': rep_id,
                'producto': combined_name,
                'producto_label': f"{combined_name} ({unit})",
                'valor': total_value,
                'volume_unit': unit,
            })

        return rows

    def _calculate_product_metrics_for_rep(
        self,
        sources: dict[str, pd.DataFrame],
        config: ProductConfig,
        rep_id: str
    ) -> tuple[float, float | None]:
        source_df = sources.get(config.source_key)

        if source_df is None or source_df.empty:
            return 0.0, None

        df = self._normalize_columns(source_df)
        df = self._filter_by_rep_id_in_df(df, rep_id)

        if df.empty:
            return 0.0, None

        if config.product_filter:
            df = self._filter_by_product(df, config.product_filter)

        if df.empty:
            return 0.0, None

        if config.name == 'TCTP':
            volume = self._count_unique_patents(df)
        else:
            volume = self._sum_column(df, config.volume_column)
        margin = self._calculate_margin(df, config.volume_column, config.contribution_column)

        return volume, margin

    def _count_unique_patents(self, df: pd.DataFrame) -> int:
        df = self._filter_by_target_period(df)
        df = self._filter_positive_volume(df)
        patent_col = 'patente'
        if patent_col not in df.columns:
            return 0
        return df[patent_col].nunique()

    @staticmethod
    def _filter_positive_volume(df: pd.DataFrame) -> pd.DataFrame:
        volume_col = 'volumen_tct_premium'
        if volume_col not in df.columns:
            return df
        df_filtered = df[pd.to_numeric(df[volume_col], errors='coerce').fillna(0) > 0].copy()
        logger.info(f"Filtered volume > 0: {len(df_filtered)} of {len(df)} rows")
        return df_filtered

    def _filter_by_target_period(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self._target_period:
            return df

        period = self._extract_period()
        target_year = period.year
        target_month = period.month

        if 'anio' not in df.columns or 'mes' not in df.columns:
            return df

        df_filtered = df[
            (df['anio'].astype(int) == target_year) &
            (df['mes'].astype(int) == target_month)
        ].copy()

        logger.info(f"Filtered by period {target_year}-{target_month}: {len(df_filtered)} of {len(df)} rows")
        return df_filtered

    @staticmethod
    def _filter_by_rep_id_in_df(df: pd.DataFrame, rep_id: str) -> pd.DataFrame:
        if REP_ID_COL not in df.columns:
            return pd.DataFrame()

        rep_id_base = rep_id.split('-')[0] if '-' in rep_id else rep_id

        if REP_ID_DV_COL in df.columns:
            df_rut = df[REP_ID_COL].astype(str).str.strip()
            df_dv = df[REP_ID_DV_COL].astype(str).str.strip()
            df_full_rut = df_rut + "-" + df_dv

            mask = (df_full_rut == rep_id) | (df_rut == rep_id_base)
        else:
            df_rut = df[REP_ID_COL].astype(str).str.strip()
            mask = (df_rut == rep_id) | (df_rut == rep_id_base)

        return df[mask].copy()

    @staticmethod
    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()

        column_mappings = {
            'vol. 2025': 'volumen',
            'con. 2025': 'contribucion',
            'rut': 'rut_ejecutivo',
        }
        df = df.rename(columns=column_mappings)

        if 'rut_ejecutivo' in df.columns and 'dv_ejecutivo' not in df.columns:
            rut_col = df['rut_ejecutivo'].astype(str)
            if rut_col.str.contains(r'\.').any():
                df['rut_ejecutivo'] = rut_col.str.replace('.', '', regex=False)

        return df

    @staticmethod
    def _filter_by_product(
        df: pd.DataFrame,
        product_filter: str | list[str]
    ) -> pd.DataFrame:
        if 'producto' not in df.columns:
            return df

        producto_normalized = df['producto'].str.upper().str.strip()

        if isinstance(product_filter, list):
            filter_upper = [p.upper().strip() for p in product_filter]
            return df[producto_normalized.isin(filter_upper)].copy()

        return df[producto_normalized == product_filter.upper().strip()].copy()

    @staticmethod
    def _sum_column(df: pd.DataFrame, column: str) -> float:
        if column not in df.columns:
            return 0.0

        return pd.to_numeric(df[column], errors='coerce').fillna(0).sum()

    @staticmethod
    def _calculate_margin(
        df: pd.DataFrame,
        volume_column: str,
        contribution_column: str | None
    ) -> float | None:
        if contribution_column is None:
            return None

        if contribution_column not in df.columns:
            return None

        total_contribution = pd.to_numeric(df[contribution_column], errors='coerce').fillna(0).sum()
        total_volume = pd.to_numeric(df[volume_column], errors='coerce').fillna(0).sum()

        if total_volume == 0:
            return None

        return total_contribution / total_volume

    def _filter_by_rep_id(self, df: pd.DataFrame) -> pd.DataFrame:
        rep_id_str = str(self._rep_id_filter).strip()
        filtered = df[df["rep_id"].astype(str).str.strip() == rep_id_str].copy()
        logger.info(f"Filtered by Rep ID {rep_id_str}: {len(filtered)} records")
        return filtered

    def _extract_period(self) -> datetime:
        if self._target_period:
            try:
                if isinstance(self._target_period, datetime):
                    return self._target_period
                if hasattr(self._target_period, 'year'):
                    return datetime(
                        self._target_period.year,
                        self._target_period.month,
                        self._target_period.day
                    )
                return datetime.strptime(str(self._target_period), '%Y-%m-%d')
            except (ValueError, AttributeError) as e:
                logger.error(f"Error parsing target_period: {e}")

        return datetime.now().replace(day=1)

    def get_column_types(self) -> dict[str, str]:
        return self._output_formatter.COLUMN_TYPES
