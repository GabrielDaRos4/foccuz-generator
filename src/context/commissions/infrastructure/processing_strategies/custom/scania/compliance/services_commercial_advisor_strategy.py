import logging

import numpy as np
import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class ServicesCommercialAdvisorStrategy(BaseScaniaStrategy):

    SALES_COMPLIANCE_THRESHOLDS = [
        (105, float("inf"), 950000),
        (104, 105.99, 850000),
        (102, 103.99, 750000),
        (100, 101.99, 650000),
        (98, 99.99, 600000),
        (96, 97.99, 550000),
        (94, 95.99, 500000),
        (92, 93.99, 450000),
        (90, 91.99, 400000),
        (0, 89.99, 0),
    ]

    VISIT_PLAN_FACTORS = [
        (100, float("inf"), 1.00),
        (95, 99.99, 0.75),
        (90, 94.99, 0.50),
        (0, 89.99, 0.00),
    ]

    CONTRACT_RATES_USD = {
        ("am", "nuevo"): 75.0,
        ("am", "nuevo parque"): 75.0,
        ("rm", "nuevo"): 125.0,
        ("rmpt", "nuevo"): 100.0,
        ("am", "renovacion"): 37.5,
        ("am", "renovación"): 37.5,
        ("rm", "renovacion"): 62.5,
        ("rm", "renovación"): 62.5,
        ("fms", "venta fms"): 40.0,
        ("fms", "fms"): 40.0,
    }

    CONTRACT_RATE_COLUMNS = {
        "_valor_contrato_rm_nuevo": 125.0,
        "_valor_contrato_rmpt_nuevo": 100.0,
        "_valor_contrato_am_nuevo_parque": 75.0,
        "_valor_contrato_am_nuevo": 75.0,
        "_valor_contrato_rm_renovacion": 62.5,
        "_valor_contrato_am_renovacion": 37.5,
    }

    DEFAULT_MINIMUM_COMMISSION = 0

    COLUMN_RENAME_MAP = {
        "venta": "Venta",
        "meta_venta": "Meta Venta",
        "cumplimiento_ventas": "Cumplimiento Ventas",
        "pago_ventas": "Pago Ventas",
        "pago_ventas_final": "Pago Ventas Final",
        "_valor_contrato_rm_nuevo": "_Valor Contrato RM Nuevo",
        "_valor_contrato_rmpt_nuevo": "_Valor Contrato RMPT Nuevo",
        "_valor_contrato_am_nuevo_parque": "_Valor Contrato AM Nuevo Parque",
        "_valor_contrato_am_nuevo": "_Valor Contrato AM Nuevo",
        "_valor_contrato_rm_renovacion": "_Valor Contrato RM Renovación",
        "_valor_contrato_am_renovacion": "_Valor Contrato AM Renovación",
        "vam": "Venta AM",
        "vrm": "Venta RM",
        "vrmpt": "Venta RMPT",
        "ram": "Renovación AM",
        "rrm": "Renovación RM",
        "vfms": "Venta FMS",
        "total_usd_vam": "Total USD Venta AM",
        "total_usd_vrm": "Total USD Venta RM",
        "total_usd_vrmpt": "Total USD Venta RMPT",
        "total_usd_ram": "Total USD Renovación AM",
        "total_usd_rrm": "Total USD Renovación RM",
        "total_usd_vfms": "Total USD Venta FMS",
        "pago_vam": "Pago Venta AM",
        "pago_vrm": "Pago Venta RM",
        "pago_vrmpt": "Pago Venta RMPT",
        "pago_ram": "Pago Renovación AM",
        "pago_rrm": "Pago Renovación RM",
        "pago_fms": "Pago Venta FMS",
        "total_usd": "Total USD",
        "total_contratos": "Total Contratos",
        "total_venta_contratos": "Total Venta Contratos",
        "n_visitas": "N° Visitas",
        "plan_visitas": "Plan de Visitas",
        "cumplimiento_visitas": "Cumplimiento Plan Visitas",
        "factor_pago": "Factor Plan de Visitas",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta Venta", "Cumplimiento Ventas", "Pago Ventas", "Pago Ventas Final",
        "_Valor Contrato RM Nuevo", "_Valor Contrato RMPT Nuevo", "_Valor Contrato AM Nuevo Parque",
        "_Valor Contrato AM Nuevo", "_Valor Contrato RM Renovación", "_Valor Contrato AM Renovación",
        "Venta AM", "Total USD Venta AM", "Pago Venta AM",
        "Venta RM", "Total USD Venta RM", "Pago Venta RM",
        "Venta RMPT", "Total USD Venta RMPT", "Pago Venta RMPT",
        "Renovación AM", "Total USD Renovación AM", "Pago Renovación AM",
        "Renovación RM", "Total USD Renovación RM", "Pago Renovación RM",
        "Venta FMS", "Total USD Venta FMS", "Pago Venta FMS",
        "Total USD", "Total Contratos", "Total Venta Contratos",
        "N° Visitas", "Plan de Visitas", "Cumplimiento Plan Visitas", "Factor Plan de Visitas",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta Venta": "money",
        "Cumplimiento Ventas": "percentage",
        "Pago Ventas": "money",
        "Pago Ventas Final": "money",
        "_Valor Contrato RM Nuevo": "money",
        "_Valor Contrato RMPT Nuevo": "money",
        "_Valor Contrato AM Nuevo Parque": "money",
        "_Valor Contrato AM Nuevo": "money",
        "_Valor Contrato RM Renovación": "money",
        "_Valor Contrato AM Renovación": "money",
        "Venta AM": "integer",
        "Venta RM": "integer",
        "Venta RMPT": "integer",
        "Renovación AM": "integer",
        "Renovación RM": "integer",
        "Venta FMS": "integer",
        "Total USD Venta AM": "decimal",
        "Total USD Venta RM": "decimal",
        "Total USD Venta RMPT": "decimal",
        "Total USD Renovación AM": "decimal",
        "Total USD Renovación RM": "decimal",
        "Total USD Venta FMS": "decimal",
        "Pago Venta AM": "money",
        "Pago Venta RM": "money",
        "Pago Venta RMPT": "money",
        "Pago Renovación AM": "money",
        "Pago Renovación RM": "money",
        "Pago Venta FMS": "money",
        "Total USD": "decimal",
        "Total Contratos": "integer",
        "Total Venta Contratos": "money",
        "N° Visitas": "integer",
        "Plan de Visitas": "integer",
        "Cumplimiento Plan Visitas": "percentage",
        "Factor Plan de Visitas": "decimal",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def __init__(self, valor_dolar: float = None, **kwargs):
        super().__init__(**kwargs)
        self._valor_dolar = valor_dolar

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': ["cargo == 'asesor comercial de servicios'"],
            'filtered_out_by_role': 0,
        }

        cargo_col = next(
            (c for c in df.columns if c.lower() == "cargo"), None
        )

        if not cargo_col:
            cargo_col = next(
                (c for c in df.columns if "cargo" in c.lower() and "cargo2" not in c.lower()), None
            )

        if not cargo_col:
            logger.warning("No cargo column found, returning empty")
            diagnostics['no_cargo_column'] = True
            return pd.DataFrame(), diagnostics

        cargo_values = (
            df[cargo_col]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("_", " ", regex=False)
        )

        mask = cargo_values == "asesor comercial de servicios"

        filtered_df = df[mask].copy()

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        logger.info(f"Filtered by cargo 'asesor comercial de servicios': {len(filtered_df)} employees")

        return filtered_df, diagnostics

    def _prepare_branch_id(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.copy()

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_valor_dolar(result)
        result = self._calculate_sales_compliance(result)
        result = self._calculate_contract_payments(result)
        result = self._calculate_visit_plan_factor(result)
        result = self._calculate_final_commission(result)

        return result

    def _extract_valor_dolar(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        if self._valor_dolar:
            result["_valor_dolar"] = self._valor_dolar
            return result

        valor_col = None
        for pattern in ["valor dolar", "valor_dolar", "valordolar"]:
            valor_col = next(
                (c for c in result.columns if pattern in c.lower().replace(" ", "")), None
            )
            if valor_col:
                break

        if not valor_col:
            valor_col = next(
                (c for c in result.columns if c.lower() == "valor"), None
            )

        if valor_col:
            result["_valor_dolar"] = pd.to_numeric(
                result[valor_col], errors="coerce"
            ).fillna(0)
            logger.info(f"Using valor dolar from column '{valor_col}': {result['_valor_dolar'].iloc[0]}")
        else:
            result["_valor_dolar"] = 900
            logger.warning("No valor dolar found, using default 900")

        return result

    def _calculate_sales_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        venta_col = self._find_column(result, "actual -is", "actual-is", "actual", "venta")
        budget_col = self._find_column(result, "budget1-is", "budget1", "budget")

        result["venta"] = self._safe_numeric(result, venta_col)
        result["meta_venta"] = self._safe_numeric(result, budget_col)

        result["cumplimiento_ventas"] = np.where(
            result["meta_venta"] > 0,
            result["venta"] / result["meta_venta"],
            0.0
        )

        result["pago_ventas"] = result["cumplimiento_ventas"].apply(
            self._get_sales_payment
        )

        days_col = self._find_column(result, "nodiastrabajados", "dias trabajados", "dias_trabajados")
        result["days_worked"] = self._safe_numeric(result, days_col)
        result.loc[result["days_worked"] <= 0, "days_worked"] = self.DAYS_PER_MONTH

        result["pago_ventas_final"] = (
            result["pago_ventas"] / self.DAYS_PER_MONTH * result["days_worked"]
        ).round(0)

        return result

    def _get_sales_payment(self, compliance: float) -> int:
        if pd.isna(compliance):
            return 0
        pct = compliance * 100
        for lower, upper, payment in self.SALES_COMPLIANCE_THRESHOLDS:
            if lower <= pct <= upper:
                return payment
        return 0

    def _calculate_contract_payments(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["vam"] = 0
        result["vrm"] = 0
        result["vrmpt"] = 0
        result["ram"] = 0
        result["rrm"] = 0
        result["vfms"] = 0

        result["total_usd_vam"] = 0.0
        result["total_usd_vrm"] = 0.0
        result["total_usd_vrmpt"] = 0.0
        result["total_usd_ram"] = 0.0
        result["total_usd_rrm"] = 0.0
        result["total_usd_vfms"] = 0.0

        valor_dolar = result["_valor_dolar"].iloc[0] if "_valor_dolar" in result.columns else 900
        for col_name, rate_usd in self.CONTRACT_RATE_COLUMNS.items():
            result[col_name] = rate_usd * valor_dolar

        contracts_df = self._get_contracts_data(result)

        if contracts_df is not None and not contracts_df.empty:
            result = self._aggregate_contracts(result, contracts_df)

        fms_df = self._get_fms_data(result)
        if fms_df is not None and not fms_df.empty:
            result = self._aggregate_fms(result, fms_df)

        valor_dolar = result["_valor_dolar"].iloc[0] if "_valor_dolar" in result.columns else 900

        result["pago_vam"] = result["total_usd_vam"] * valor_dolar
        result["pago_vrm"] = result["total_usd_vrm"] * valor_dolar
        result["pago_vrmpt"] = result["total_usd_vrmpt"] * valor_dolar
        result["pago_ram"] = result["total_usd_ram"] * valor_dolar
        result["pago_rrm"] = result["total_usd_rrm"] * valor_dolar
        result["pago_fms"] = result["total_usd_vfms"] * valor_dolar

        result["total_usd"] = (
            result["total_usd_vam"] +
            result["total_usd_vrm"] +
            result["total_usd_vrmpt"] +
            result["total_usd_ram"] +
            result["total_usd_rrm"] +
            result["total_usd_vfms"]
        ).fillna(0)

        result["total_contratos"] = (
            result["vam"] + result["vrm"] + result["vrmpt"] +
            result["ram"] + result["rrm"] + result["vfms"]
        ).fillna(0).astype(int)

        result["total_venta_contratos"] = (
            result["pago_vam"] + result["pago_vrm"] + result["pago_vrmpt"] +
            result["pago_ram"] + result["pago_rrm"] + result["pago_fms"]
        ).fillna(0)

        return result

    def _get_contracts_data(self, df: pd.DataFrame) -> pd.DataFrame | None:
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        for key in ['Venta de contratos AM_RM', 'venta de contratos am_rm', 'contratos_amrm']:
            if key in secondary_arrays:
                return secondary_arrays[key].copy()

        for col in df.columns:
            if 'producto' in col.lower():
                return None

        return None

    def _get_fms_data(self, df: pd.DataFrame) -> pd.DataFrame | None:
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        for key in ['Venta de contratos FMS', 'venta de contratos fms', 'contratos_fms']:
            if key in secondary_arrays:
                return secondary_arrays[key].copy()

        return None

    def _aggregate_contracts(self, df: pd.DataFrame, contracts: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        contracts = contracts.copy()
        contracts.columns = contracts.columns.str.lower().str.strip()

        rut_col = next((c for c in contracts.columns if "rut" in c.lower()), None)
        if not rut_col:
            return result

        contracts["_rut_clean"] = self._normalize_rut(contracts[rut_col])

        producto_col = next((c for c in contracts.columns if "producto" in c.lower()), None)
        tipo_col = next((c for c in contracts.columns if "tipo" in c.lower()), None)

        if not producto_col or not tipo_col:
            return result

        contracts["_producto"] = contracts[producto_col].astype(str).str.lower().str.strip()
        contracts["_tipo"] = contracts[tipo_col].astype(str).str.lower().str.strip()

        contracts["_monto_usd"] = contracts.apply(
            lambda r: self._get_contract_rate(r["_producto"], r["_tipo"]),
            axis=1
        )

        emp_rut_col = next((c for c in result.columns if "rut" in c.lower()), None)
        if not emp_rut_col:
            return result

        result["_rut_clean"] = self._normalize_rut(result[emp_rut_col])

        contract_types = [
            ("vam", "am", "nuevo", ["nuevo", "nuevo parque"]),
            ("vrm", "rm", "nuevo", ["nuevo"]),
            ("vrmpt", "rmpt", "nuevo", ["nuevo"]),
            ("ram", "am", "renov", ["renovacion", "renovación"]),
            ("rrm", "rm", "renov", ["renovacion", "renovación"]),
        ]

        for col_name, producto_pattern, tipo_pattern, _tipo_matches in contract_types:
            if producto_pattern == "rm" and col_name != "rrm":
                mask = (
                    contracts["_producto"].str.contains(producto_pattern, na=False) &
                    ~contracts["_producto"].str.contains("rmpt", na=False) &
                    contracts["_tipo"].str.contains(tipo_pattern, na=False)
                )
            elif producto_pattern == "rm" and col_name == "rrm":
                mask = (
                    contracts["_producto"].str.contains(producto_pattern, na=False) &
                    ~contracts["_producto"].str.contains("rmpt", na=False) &
                    contracts["_tipo"].str.contains(tipo_pattern, na=False)
                )
            else:
                mask = (
                    contracts["_producto"].str.contains(producto_pattern, na=False) &
                    contracts["_tipo"].str.contains(tipo_pattern, na=False)
                )

            subset = contracts[mask]
            if not subset.empty:
                agg = subset.groupby("_rut_clean").agg(
                    count=("_monto_usd", "count"),
                    total_usd=("_monto_usd", "sum")
                ).reset_index()

                for idx, row in result.iterrows():
                    rut = row["_rut_clean"]
                    match = agg[agg["_rut_clean"] == rut]
                    if not match.empty:
                        result.loc[idx, col_name] = match["count"].iloc[0]
                        result.loc[idx, f"total_usd_{col_name}"] = match["total_usd"].iloc[0]

        result = result.drop(columns=["_rut_clean"], errors="ignore")
        return result

    def _aggregate_fms(self, df: pd.DataFrame, fms: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        fms = fms.copy()
        fms.columns = fms.columns.str.lower().str.strip()

        rut_col = next((c for c in fms.columns if "rut" in c.lower()), None)
        if not rut_col:
            return result

        fms["_rut_clean"] = self._normalize_rut(fms[rut_col])

        fms_rate = 40.0

        agg = fms.groupby("_rut_clean").size().reset_index(name="count")
        agg["total_usd"] = agg["count"] * fms_rate

        emp_rut_col = next((c for c in result.columns if "rut" in c.lower()), None)
        if not emp_rut_col:
            return result

        result["_rut_clean"] = self._normalize_rut(result[emp_rut_col])

        for idx, row in result.iterrows():
            rut = row["_rut_clean"]
            match = agg[agg["_rut_clean"] == rut]
            if not match.empty:
                result.loc[idx, "vfms"] = match["count"].iloc[0]
                result.loc[idx, "total_usd_vfms"] = match["total_usd"].iloc[0]

        result = result.drop(columns=["_rut_clean"], errors="ignore")
        return result

    def _get_contract_rate(self, producto: str, tipo: str) -> float:
        producto = str(producto).strip().lower()
        tipo = str(tipo).strip().lower()

        key = (producto, tipo)
        if key in self.CONTRACT_RATES_USD:
            return self.CONTRACT_RATES_USD[key]

        if "am" in producto:
            if "nuevo" in tipo:
                return 75.0
            elif "renov" in tipo:
                return 37.5
        elif "rmpt" in producto:
            if "nuevo" in tipo:
                return 100.0
        elif "rm" in producto:
            if "nuevo" in tipo:
                return 125.0
            elif "renov" in tipo:
                return 62.5
        elif "fms" in producto:
            return 40.0

        return 0.0

    def _normalize_rut(self, series: pd.Series) -> pd.Series:
        def clean_rut(rut: str) -> str:
            original = str(rut).strip()
            has_hyphen = "-" in original
            cleaned = original.replace(".", "").replace("-", "")
            if has_hyphen and len(cleaned) >= 8:
                return cleaned[:-1]
            return cleaned

        return series.apply(clean_rut)

    def _calculate_visit_plan_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._count_visits_from_array(result)
        result = self._enrich_visit_plan_from_array(result)

        result["cumplimiento_visitas"] = np.where(
            result["plan_visitas"] > 0,
            result["n_visitas"] / result["plan_visitas"],
            0.0
        )

        result["factor_pago"] = result["cumplimiento_visitas"].apply(
            self._get_visit_factor
        )

        return result

    def _enrich_visit_plan_from_array(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["plan_visitas"] = 0

        secondary_arrays = result.attrs.get('secondary_arrays', {})

        meta_df = None
        for key in ['Meta Plan de visitas de Asesores', 'Meta Plan de visitas', 'meta visitas']:
            if key in secondary_arrays:
                meta_df = secondary_arrays[key].copy()
                break

        if meta_df is None or meta_df.empty:
            logger.warning("No visit plan meta data found in secondary arrays")
            return result

        meta_df.columns = meta_df.columns.str.lower().str.strip()

        rut_col = next((c for c in meta_df.columns if "rut" in c.lower()), None)
        meta_col = next((c for c in meta_df.columns if c.lower() == "meta"), None)
        days_col = next((c for c in meta_df.columns if "diastraba" in c.lower().replace(" ", "")), None)

        if not rut_col or not meta_col:
            logger.warning(f"Missing columns in visit plan meta: rut={rut_col}, meta={meta_col}")
            return result

        meta_df["_rut_clean"] = self._normalize_rut(meta_df[rut_col])

        emp_rut_col = next((c for c in result.columns if "rut" in c.lower()), None)
        if not emp_rut_col:
            return result

        result["_rut_clean"] = self._normalize_rut(result[emp_rut_col])

        for idx, row in result.iterrows():
            rut = row["_rut_clean"]
            match = meta_df[meta_df["_rut_clean"] == rut]
            if not match.empty:
                result.loc[idx, "plan_visitas"] = pd.to_numeric(
                    match[meta_col].iloc[0], errors="coerce"
                ) or 0
                if days_col and "days_worked" in result.columns:
                    days_val = pd.to_numeric(match[days_col].iloc[0], errors="coerce")
                    if days_val and days_val > 0:
                        result.loc[idx, "days_worked"] = int(days_val)

        result = result.drop(columns=["_rut_clean"], errors="ignore")
        logger.info(f"Enriched visit plan meta for {len(result)} employees")
        return result

    def _count_visits_from_array(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["n_visitas"] = 0

        secondary_arrays = result.attrs.get('secondary_arrays', {})

        visits_df = None
        for key in ['Visitas de Asesores a clientes', 'visitas de asesores', 'visitas']:
            if key in secondary_arrays:
                visits_df = secondary_arrays[key].copy()
                break

        if visits_df is None or visits_df.empty:
            logger.warning("No visits data found in secondary arrays")
            return result

        visits_df.columns = visits_df.columns.str.lower().str.strip()

        rut_col = next((c for c in visits_df.columns if "rut" in c.lower()), None)
        if not rut_col:
            logger.warning("No RUT column found in visits data")
            return result

        visits_df["_rut_clean"] = self._normalize_rut(visits_df[rut_col])

        emp_rut_col = next((c for c in result.columns if "rut" in c.lower()), None)
        if not emp_rut_col:
            return result

        result["_rut_clean"] = self._normalize_rut(result[emp_rut_col])

        visit_counts = visits_df.groupby("_rut_clean").size().reset_index(name="count")

        for idx, row in result.iterrows():
            rut = row["_rut_clean"]
            match = visit_counts[visit_counts["_rut_clean"] == rut]
            if not match.empty:
                result.loc[idx, "n_visitas"] = match["count"].iloc[0]

        result = result.drop(columns=["_rut_clean"], errors="ignore")
        logger.info(f"Counted visits for {len(result)} employees from visits array")
        return result

    def _get_visit_factor(self, compliance: float) -> float:
        if pd.isna(compliance):
            return 0.0
        pct = compliance * 100
        for lower, upper, factor in self.VISIT_PLAN_FACTORS:
            if lower <= pct <= upper:
                return factor
        return 0.0

    def _calculate_final_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result["commission"] = (
            (result["total_venta_contratos"] + result["pago_ventas_final"]) *
            result["factor_pago"]
        ).fillna(0)

        result["commission"] = (
            result["commission"] / self.DAYS_PER_MONTH * result["days_worked"]
        ).round(0)

        result = self._apply_guaranteed_minimum(result)

        return result

    def _apply_guaranteed_minimum(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        guaranteed_col = self._find_guaranteed_column(result)

        if guaranteed_col:
            result["guaranteed"] = pd.to_numeric(
                result[guaranteed_col].replace("NA", 0), errors="coerce"
            ).fillna(0)
        else:
            result["guaranteed"] = self.DEFAULT_MINIMUM_COMMISSION

        result["commission"] = result[["commission", "guaranteed"]].max(axis=1)
        return result

    def _find_guaranteed_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "garantizado" in col.lower():
                return col
        return None

    def _safe_numeric(self, df: pd.DataFrame, col: str | None) -> pd.Series:
        if col and col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0)
        return pd.Series([0] * len(df), index=df.index)

    def _find_column(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower().replace(" ", "") in col.lower().replace(" ", ""):
                    return col
        return None


AsesorComercialServiciosStrategy = ServicesCommercialAdvisorStrategy
