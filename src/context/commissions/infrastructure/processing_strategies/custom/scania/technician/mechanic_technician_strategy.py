import logging
import re

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class MechanicTechnicianStrategy(BaseScaniaStrategy):

    EXCLUDED_EMPLOYEE_IDS = [
        5377, 5798, 5886, 5964, 6229, 5446, 7248, 5458, 8568, 5984,
        6001, 5959, 7742, 6008, 7544, 6014, 5932, 6007, 6389, 5834,
        7940, 8173, 8601, 8670, 8633, 6015, 9392, 9425
    ]

    SALES_COMPLIANCE_THRESHOLDS = [
        (100, 104.99, 50000),
        (105, 109.99, 70000),
        (110, 114.99, 90000),
        (115, 119.99, 120000),
        (120, float("inf"), 150000),
    ]

    PRODUCTIVITY_THRESHOLDS = [
        (83, 84.99, 37000),
        (85, 89.99, 50000),
        (90, 94.99, 70000),
        (95, 99.99, 90000),
        (100, float("inf"), 120000),
    ]

    EFFICIENCY_THRESHOLDS_BY_LEVEL = {
        "i": [
            (90, 94.99, 35000),
            (95, 99.99, 50000),
            (100, 104.99, 80000),
            (105, 109.99, 100000),
            (110, float("inf"), 120000),
        ],
        "ii": [
            (90, 94.99, 35000),
            (95, 99.99, 50000),
            (100, 104.99, 100000),
            (105, 109.99, 120000),
            (110, float("inf"), 140000),
        ],
        "iii": [
            (90, 94.99, 35000),
            (95, 99.99, 50000),
            (100, 104.99, 120000),
            (105, 109.99, 140000),
            (110, float("inf"), 160000),
        ],
    }

    NPS_THRESHOLDS = [
        (80, 85.99, 0.00),
        (86, 89.99, 0.15),
        (90, 94.99, 0.20),
        (95, 99.99, 0.25),
        (100, float("inf"), 0.30),
    ]

    COLUMN_RENAME_MAP = {
        "cumplimiento_venta": "Cumplimiento Venta",
        "pago_cumplimiento_venta": "Pago Cumplimiento Venta",
        "cumplimiento_productividad": "Cumplimiento Productividad",
        "meta_venta": "Meta Venta",
        "resultado_venta": "Resultado Venta",
        "pago_productividad": "Pago Productividad",
        "cumplimiento_eficiencia": "Cumplimiento Eficiencia",
        "pago_eficiencia": "Pago Eficiencia",
        "resultado_nps": "Resultado NPS",
        "cumplimiento_nps": "Cumplimiento NPS",
        "n_retornos": "N° Retornos",
        "monto_retornos": "Monto Retornos",
        "cumplimiento_retornos": "Cumplimiento Retornos",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Cumplimiento Productividad", "Meta Venta", "Resultado Venta",
        "Pago Productividad", "Cumplimiento Eficiencia", "Pago Eficiencia",
        "Resultado NPS", "Cumplimiento NPS",
        "N° Retornos", "Monto Retornos", "Cumplimiento Retornos",
        "Comisión"
    ]

    COLUMN_TYPES = {
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Cumplimiento Productividad": "percentage",
        "Meta Venta": "money",
        "Resultado Venta": "money",
        "Pago Productividad": "money",
        "Cumplimiento Eficiencia": "percentage",
        "Pago Eficiencia": "money",
        "Resultado NPS": "percentage",
        "Cumplimiento NPS": "percentage",
        "N° Retornos": "integer",
        "Monto Retornos": "money",
        "Cumplimiento Retornos": "percentage",
        "Comisión": "money",
    }

    def _filter_by_role_with_diagnostics(
        self, df: pd.DataFrame
    ) -> tuple[pd.DataFrame, dict]:
        diagnostics = {
            'input_rows': len(df),
            'role_filter': self._role_filter,
            'filtered_out_by_role': 0,
            'available_roles': [],
        }

        role_col = self._find_role_column(df)
        if not role_col:
            logger.warning("No role column found, returning all employees")
            diagnostics['no_role_column'] = True
            return df.copy(), diagnostics

        role_values = df[role_col].astype(str).str.strip().str.lower()
        diagnostics['available_roles'] = role_values.unique().tolist()[:10]

        mask = role_values.str.contains("tecnico_mecanico|tecnico mecanico", regex=True, na=False)
        filtered_df = df[mask].copy()
        filtered_df.attrs = df.attrs.copy()

        if self.EXCLUDED_EMPLOYEE_IDS:
            id_col = self._find_employee_id_column(filtered_df)
            if id_col:
                employee_ids = pd.to_numeric(filtered_df[id_col], errors="coerce")
                excluded_mask = employee_ids.isin(self.EXCLUDED_EMPLOYEE_IDS)
                excluded_count = excluded_mask.sum()
                if excluded_count > 0:
                    original_attrs = filtered_df.attrs.copy()
                    filtered_df = filtered_df[~excluded_mask].copy()
                    filtered_df.attrs = original_attrs
                    logger.info(f"Excluded {excluded_count} employees by ID")

        diagnostics['filtered_out_by_role'] = len(df) - len(filtered_df)
        diagnostics['matched_rows'] = len(filtered_df)

        return filtered_df, diagnostics

    def _find_employee_id_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "id" in col.lower() and "empleado" in col.lower():
                return col
        return None

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result = self._enrich_from_secondary_arrays(result)
        result = self._calculate_sales_compliance_payment(result)
        result = self._calculate_productivity_payment(result)
        result = self._calculate_efficiency_payment(result)
        result = self._calculate_nps_compliance(result)
        result = self._calculate_returns_compliance(result)
        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)
        return result

    def _enrich_from_secondary_arrays(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        secondary_arrays = df.attrs.get('secondary_arrays', {})

        emp_rut_col = next((c for c in result.columns if 'rut' in c.lower()), None)
        if not emp_rut_col:
            logger.warning("No RUT column found in employee data")
            return result

        from src.context.commissions.infrastructure.processing_strategies.custom.scania.merge.normalizers import (
            normalize_rut,
        )

        result["_rut_merge"] = normalize_rut(result[emp_rut_col])

        result = self._enrich_productividad(result, secondary_arrays, normalize_rut)
        result = self._enrich_eficiencia(result, secondary_arrays, normalize_rut)
        result = self._enrich_nps(result, secondary_arrays)
        result = self._enrich_retornos(result, secondary_arrays, normalize_rut)

        result = result.drop(columns=["_rut_merge"], errors="ignore")
        return result

    def _enrich_productividad(self, result: pd.DataFrame, secondary_arrays: dict, normalize_rut) -> pd.DataFrame:
        prod_df = secondary_arrays.get('Productividad')
        if prod_df is None or prod_df.empty:
            result["cumplimiento_productividad"] = 0
            return result

        prod_df = prod_df.copy()
        prod_df.columns = prod_df.columns.str.lower().str.strip()

        rut_col = next((c for c in prod_df.columns if c == 'rut'), None)
        prod_col = next((c for c in prod_df.columns if 'product' in c.lower()), None)

        if not rut_col or not prod_col:
            result["cumplimiento_productividad"] = 0
            return result

        prod_df["_rut_merge"] = normalize_rut(prod_df[rut_col])
        prod_df["cumplimiento_productividad"] = pd.to_numeric(prod_df[prod_col], errors="coerce").fillna(0)

        merge_cols = ["_rut_merge", "cumplimiento_productividad"]
        prod_subset = prod_df[merge_cols].drop_duplicates(subset=["_rut_merge"])
        result = result.merge(prod_subset, on="_rut_merge", how="left")

        logger.info("Enriched with Productividad")
        return result

    def _enrich_eficiencia(self, result: pd.DataFrame, secondary_arrays: dict, normalize_rut) -> pd.DataFrame:
        efic_df = secondary_arrays.get('Eficiencia')
        if efic_df is None or efic_df.empty:
            result["cumplimiento_eficiencia"] = 0
            return result

        efic_df = efic_df.copy()
        efic_df.columns = efic_df.columns.str.lower().str.strip()

        rut_col = next((c for c in efic_df.columns if c == 'rut'), None)
        efic_col = next((c for c in efic_df.columns if 'eficiencia' in c.lower()), None)

        if not rut_col or not efic_col:
            result["cumplimiento_eficiencia"] = 0
            return result

        efic_df["_rut_merge"] = normalize_rut(efic_df[rut_col])
        efic_df["cumplimiento_eficiencia"] = pd.to_numeric(efic_df[efic_col], errors="coerce").fillna(0)

        merge_cols = ["_rut_merge", "cumplimiento_eficiencia"]
        efic_subset = efic_df[merge_cols].drop_duplicates(subset=["_rut_merge"])
        result = result.merge(efic_subset, on="_rut_merge", how="left")

        logger.info("Enriched with Eficiencia")
        return result

    def _enrich_nps(self, result: pd.DataFrame, secondary_arrays: dict) -> pd.DataFrame:
        nps_col = next((c for c in result.columns if c.lower() == 'nps'), None)
        if nps_col:
            nps_values = pd.to_numeric(result[nps_col], errors="coerce").fillna(0)
            if nps_values.max() > 2:
                nps_values = nps_values / 100
            result["resultado_nps"] = nps_values
            logger.info("NPS data found in primary dataframe")
            return result

        nps_df = secondary_arrays.get('NPS')
        if nps_df is None or nps_df.empty:
            result["resultado_nps"] = 0
            return result

        nps_df = nps_df.copy()
        nps_df.columns = nps_df.columns.str.lower().str.strip()

        branch_col = next((c for c in nps_df.columns if c == 'branchid'), None)
        nps_value_col = next((c for c in nps_df.columns if 'nps' in c.lower()), None)

        if not branch_col or not nps_value_col:
            result["resultado_nps"] = 0
            return result

        emp_branch_col = next((c for c in result.columns if c.lower() == 'branchid'), None)
        if not emp_branch_col:
            result["resultado_nps"] = 0
            return result

        nps_df["_branch_merge"] = nps_df[branch_col].astype(str).str.strip().str.upper()
        nps_df["resultado_nps"] = pd.to_numeric(nps_df[nps_value_col], errors="coerce").fillna(0) / 100

        result["_branch_merge"] = result[emp_branch_col].astype(str).str.strip().str.upper()

        merge_cols = ["_branch_merge", "resultado_nps"]
        nps_subset = nps_df[merge_cols].drop_duplicates(subset=["_branch_merge"])
        result = result.merge(nps_subset, on="_branch_merge", how="left")
        result = result.drop(columns=["_branch_merge"], errors="ignore")

        logger.info("Enriched with NPS from secondary array")
        return result

    def _enrich_retornos(self, result: pd.DataFrame, secondary_arrays: dict, normalize_rut) -> pd.DataFrame:
        ret_df = secondary_arrays.get('RetornosServicio')
        if ret_df is None or ret_df.empty:
            result["n_retornos"] = 0
            result["monto_retornos"] = 0
            return result

        ret_df = ret_df.copy()
        ret_df.columns = ret_df.columns.str.lower().str.strip()

        rut_col = next((c for c in ret_df.columns if 'ruttecnico' in c.lower().replace(' ', '')), None)
        total_col = next((c for c in ret_df.columns if 'total' in c.lower()), None)

        if not rut_col or not total_col:
            result["n_retornos"] = 0
            result["monto_retornos"] = 0
            return result

        ret_df["_rut_merge"] = normalize_rut(ret_df[rut_col])
        ret_df["_total"] = pd.to_numeric(ret_df[total_col], errors="coerce").fillna(0)

        aggregated = ret_df.groupby("_rut_merge", as_index=False).agg(
            n_retornos=("_rut_merge", "size"),
            monto_retornos=("_total", "sum")
        )

        result = result.merge(aggregated, on="_rut_merge", how="left")
        result["n_retornos"] = result["n_retornos"].fillna(0)
        result["monto_retornos"] = result["monto_retornos"].fillna(0)

        logger.info("Enriched with RetornosServicio")
        return result

    def _calculate_sales_compliance_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        cumpl_col = self._find_column_by_patterns(result, ["% cumplimiento", "cumplimiento"])
        actual_col = self._find_column_by_patterns(result, ["actual", "actual -is", "actual-is"])
        budget_col = self._find_column_by_patterns(result, ["budget", "budget1", "budget1-is"])

        if cumpl_col:
            compliance_raw = pd.to_numeric(result[cumpl_col], errors="coerce").fillna(0)
            if compliance_raw.max() > 2:
                compliance_raw = compliance_raw / 100
            result["cumplimiento_venta"] = compliance_raw
        else:
            result["cumplimiento_venta"] = 0

        if actual_col:
            result["resultado_venta"] = pd.to_numeric(result[actual_col], errors="coerce").fillna(0)
        else:
            result["resultado_venta"] = 0

        if budget_col:
            result["meta_venta"] = pd.to_numeric(result[budget_col], errors="coerce").fillna(0)
        else:
            result["meta_venta"] = 0

        result["pago_cumplimiento_venta"] = (result["cumplimiento_venta"] * 100).apply(
            self._get_sales_compliance_payment
        )

        return result

    def _find_column_by_patterns(self, df: pd.DataFrame, patterns: list) -> str | None:
        for col in df.columns:
            col_lower = col.lower().strip()
            for pattern in patterns:
                if pattern in col_lower:
                    return col
        return None

    def _get_sales_compliance_payment(self, value: float) -> int:
        if pd.isna(value):
            return 0
        for lower, upper, payment in self.SALES_COMPLIANCE_THRESHOLDS:
            if lower <= value <= upper:
                return payment
        return 0

    def _calculate_productivity_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        if "cumplimiento_productividad" not in result.columns:
            result["cumplimiento_productividad"] = 0

        prod_values = result["cumplimiento_productividad"].fillna(0)
        if prod_values.max() <= 2:
            prod_pct = prod_values * 100
        else:
            prod_pct = prod_values

        result["pago_productividad"] = prod_pct.apply(self._get_productivity_payment)

        return result

    def _get_productivity_payment(self, value: float) -> int:
        if pd.isna(value):
            return 0
        for lower, upper, payment in self.PRODUCTIVITY_THRESHOLDS:
            if lower <= value <= upper:
                return payment
        return 0

    def _calculate_efficiency_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        if "cumplimiento_eficiencia" not in result.columns:
            result["cumplimiento_eficiencia"] = 0

        cargo_col = self._find_role_column(result)

        def calculate_for_row(row):
            efficiency = row.get("cumplimiento_eficiencia", 0)
            cargo = row.get(cargo_col, "") if cargo_col else ""
            return self._get_efficiency_payment(efficiency, cargo)

        result["pago_eficiencia"] = result.apply(calculate_for_row, axis=1)

        return result

    def _get_efficiency_payment(self, efficiency: float, cargo: str) -> int:
        if pd.isna(efficiency) or pd.isna(cargo):
            return 0

        if efficiency <= 2:
            efficiency_pct = efficiency * 100
        else:
            efficiency_pct = efficiency

        cargo_lower = str(cargo).lower().strip()
        cargo_normalized = (
            cargo_lower
            .replace("á", "a").replace("é", "e").replace("í", "i")
            .replace("ó", "o").replace("ú", "u")
        )

        match = re.search(r"mecanico[\s_]+(i{1,3})(?:[\s_]|$)", cargo_normalized)
        nivel = match.group(1) if match else None

        if nivel not in self.EFFICIENCY_THRESHOLDS_BY_LEVEL:
            return 0

        for lower, upper, payment in self.EFFICIENCY_THRESHOLDS_BY_LEVEL[nivel]:
            if lower <= efficiency_pct <= upper:
                return payment
        return 0

    def _calculate_nps_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        if "resultado_nps" not in result.columns:
            result["resultado_nps"] = 0

        nps_values = result["resultado_nps"].fillna(0)
        if nps_values.max() <= 2:
            nps_pct = nps_values * 100
        else:
            nps_pct = nps_values

        result["cumplimiento_nps"] = nps_pct.apply(self._get_nps_compliance)

        return result

    def _get_nps_compliance(self, value: float) -> float:
        if pd.isna(value):
            return 0
        for lower, upper, factor in self.NPS_THRESHOLDS:
            if lower <= value <= upper:
                return factor
        return 0

    def _calculate_returns_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        if "n_retornos" not in result.columns:
            result["n_retornos"] = 0
        if "monto_retornos" not in result.columns:
            result["monto_retornos"] = 0

        def get_returns_compliance(row):
            n_ret = row.get("n_retornos", 0)
            monto = row.get("monto_retornos", 0)

            if pd.isna(n_ret) or pd.isna(monto):
                return 0

            if monto < 1_000_000:
                if n_ret <= 1:
                    return 1.00
                elif n_ret == 2:
                    return 0.50
                else:
                    return 0.00
            else:
                return 0.00

        result["cumplimiento_retornos"] = result.apply(get_returns_compliance, axis=1)

        return result

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        pago_cumpl = result["pago_cumplimiento_venta"].fillna(0)
        pago_prod = result["pago_productividad"].fillna(0)
        pago_efic = result["pago_eficiencia"].fillna(0)
        cumpl_nps = result["cumplimiento_nps"].fillna(0)
        cumpl_ret = result["cumplimiento_retornos"].fillna(0)

        result["commission"] = (
            (pago_cumpl + pago_prod + pago_efic)
            * (1 + cumpl_nps)
            * cumpl_ret
        )

        result = self._apply_days_proration(result, "commission")

        return result

    def _apply_guaranteed_minimum(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        guaranteed_col = self._find_guaranteed_column(result)

        if guaranteed_col:
            result["guaranteed"] = pd.to_numeric(
                result[guaranteed_col].replace("NA", 0), errors="coerce"
            ).fillna(0)
        else:
            result["guaranteed"] = 0

        result["commission"] = result[["commission", "guaranteed"]].max(axis=1)
        return result

    def _find_guaranteed_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            if "garantizado" in col.lower():
                return col
        return None


TecnicoMecanicoStrategy = MechanicTechnicianStrategy
