import logging

import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania.base_scania_strategy import (
    BaseScaniaStrategy,
)

logger = logging.getLogger(__name__)


class WorkshopManagerStrategy(BaseScaniaStrategy):

    SALES_THRESHOLDS = [
        (1.20, float("inf"), 750000),
        (1.15, 1.1999, 650000),
        (1.10, 1.1499, 550000),
        (1.05, 1.0999, 450000),
        (1.00, 1.0499, 400000),
        (0, 0.9999, 0),
    ]

    PRODUCTIVITY_THRESHOLDS = [
        (1.00, float("inf"), 150000),
        (0.95, 0.9999, 120000),
        (0.90, 0.9499, 90000),
        (0.85, 0.8999, 60000),
        (0.83, 0.8499, 42000),
        (0, 0.8299, 0),
    ]

    EFFICIENCY_THRESHOLDS = [
        (1.10, float("inf"), 180000),
        (1.05, 1.0999, 150000),
        (1.00, 1.0499, 120000),
        (0.95, 0.9999, 90000),
        (0.90, 0.9499, 60000),
        (0, 0.8999, 0),
    ]

    NPS_THRESHOLDS = [
        (1.00, float("inf"), 0.30),
        (0.96, 0.9999, 0.25),
        (0.91, 0.9599, 0.20),
        (0.86, 0.9099, 0.15),
        (0.80, 0.8599, 0.00),
        (0, 0.7999, 0.00),
    ]

    COLUMN_RENAME_MAP = {
        "venta": "Venta",
        "meta": "Meta",
        "cumplimiento venta": "Cumplimiento Venta",
        "pago_cumplimiento_venta": "Pago Cumplimiento Venta",
        "cumplimiento productividad": "Cumplimiento Productividad",
        "pago_productividad": "Pago Productividad",
        "cumplimiento eficiencia": "Cumplimiento Eficiencia",
        "pago_eficiencia": "Pago Eficiencia",
        "resultado nps": "Resultado NPS",
        "cumplimiento_nps": "Cumplimiento NPS",
        "n retornos": "N Retornos",
        "monto retornos": "Monto Retornos",
        "cumplimiento_retornos": "Cumplimiento Retornos",
        "days_worked": "Días Trabajados",
        "commission": "Comisión",
    }

    PLAN_OUTPUT_COLUMNS = [
        "Venta", "Meta", "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Cumplimiento Productividad", "Pago Productividad",
        "Cumplimiento Eficiencia", "Pago Eficiencia",
        "Resultado NPS", "Cumplimiento NPS",
        "N Retornos", "Monto Retornos", "Cumplimiento Retornos",
        "Días Trabajados", "Comisión"
    ]

    COLUMN_TYPES = {
        "Venta": "money",
        "Meta": "money",
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Cumplimiento Productividad": "percentage",
        "Pago Productividad": "money",
        "Cumplimiento Eficiencia": "percentage",
        "Pago Eficiencia": "money",
        "Resultado NPS": "percentage",
        "Cumplimiento NPS": "percentage",
        "N Retornos": "integer",
        "Monto Retornos": "money",
        "Cumplimiento Retornos": "percentage",
        "Días Trabajados": "integer",
        "Comisión": "money",
    }

    def _calculate_plan_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        result = self._extract_sales_data(result)
        result = self._extract_productivity_data(result)
        result = self._extract_efficiency_data(result)
        result = self._extract_nps_data(result)
        result = self._extract_returns_data(result)

        result = self._calculate_sales_payment(result)
        result = self._calculate_productivity_payment(result)
        result = self._calculate_efficiency_payment(result)
        result = self._calculate_nps_compliance(result)
        result = self._calculate_returns_compliance(result)

        result = self._calculate_total_commission(result)
        result = self._apply_guaranteed_minimum(result)

        return result

    def _extract_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        compliance_col = self._find_compliance_column(result)
        if compliance_col:
            compliance = pd.to_numeric(result[compliance_col], errors="coerce").fillna(0)
            if compliance.max() > 2:
                compliance = compliance / 100
            result["cumplimiento venta"] = compliance
        else:
            result["cumplimiento venta"] = 0

        sales_col = self._find_sales_column(result)
        if sales_col:
            result["venta"] = pd.to_numeric(result[sales_col], errors="coerce").fillna(0)
        elif "venta" not in result.columns:
            result["venta"] = 0

        budget_col = self._find_budget_column(result)
        if budget_col:
            result["meta"] = pd.to_numeric(result[budget_col], errors="coerce").fillna(0)
        elif "meta" not in result.columns:
            result["meta"] = 0

        return result

    def _find_compliance_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "cumplimiento" in col_lower and "productividad" not in col_lower and "eficiencia" not in col_lower:
                return col
        return None

    def _find_sales_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "cumplimiento" in col_lower:
                continue
            if "actual" in col_lower or col_lower == "venta":
                return col
        return None

    def _find_budget_column(self, df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "budget" in col_lower or "meta" in col_lower:
                return col
        return None

    def _extract_productivity_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        prod_col = self._find_column(result, "productividad")
        if prod_col:
            prod = pd.to_numeric(result[prod_col], errors="coerce").fillna(0)
            if prod.max() > 2:
                prod = prod / 100
            result["cumplimiento productividad"] = prod
        else:
            result["cumplimiento productividad"] = 0

        return result

    def _extract_efficiency_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        eff_col = self._find_column(result, "eficiencia")
        if eff_col:
            eff = pd.to_numeric(result[eff_col], errors="coerce").fillna(0)
            if eff.max() > 2:
                eff = eff / 100
            result["cumplimiento eficiencia"] = eff
        else:
            result["cumplimiento eficiencia"] = 0

        return result

    def _extract_nps_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        nps_col = self._find_column(result, "nps")
        if nps_col:
            nps = pd.to_numeric(result[nps_col], errors="coerce").fillna(0)
            if nps.max() > 2:
                nps = nps / 100
            result["resultado nps"] = nps
        else:
            result["resultado nps"] = 0

        return result

    def _extract_returns_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result.attrs = df.attrs.copy()

        n_retornos_col = self._find_column(result, "n retornos", "nretornos")
        monto_col = self._find_column(result, "monto retornos", "montoretornos")

        if n_retornos_col and monto_col:
            result["n retornos"] = pd.to_numeric(result[n_retornos_col], errors="coerce").fillna(0)
            result["monto retornos"] = pd.to_numeric(result[monto_col], errors="coerce").fillna(0)
            return result

        secondary_arrays = result.attrs.get('secondary_arrays', {})

        retornos_df = None
        for key, sec_df in secondary_arrays.items():
            if "retorno" in key.lower():
                retornos_df = sec_df.copy()
                break

        if retornos_df is not None and not retornos_df.empty:
            result = self._merge_retornos_by_branch(result, retornos_df)
        else:
            result["n retornos"] = 0
            result["monto retornos"] = 0

        return result

    def _merge_retornos_by_branch(
        self, employees_df: pd.DataFrame, retornos_df: pd.DataFrame
    ) -> pd.DataFrame:
        retornos_df = retornos_df.copy()
        retornos_df.columns = retornos_df.columns.str.lower().str.strip()

        branchid_col = next(
            (c for c in retornos_df.columns if "branchid" in c.lower() and "reparador" in c.lower()),
            None
        )
        if not branchid_col:
            branchid_col = next(
                (c for c in retornos_df.columns if "branchid" in c.lower()),
                None
            )

        total_col = next(
            (c for c in retornos_df.columns if c == "total"),
            None
        )

        if not branchid_col or not total_col:
            employees_df["n retornos"] = 0
            employees_df["monto retornos"] = 0
            return employees_df

        retornos_df[total_col] = pd.to_numeric(retornos_df[total_col], errors="coerce").fillna(0)
        retornos_df["_branchid"] = retornos_df[branchid_col].astype(str).str.strip().str.upper()

        aggregated = (
            retornos_df.groupby("_branchid", as_index=False)
            .agg(
                n_retornos=(total_col, "count"),
                monto_retornos=(total_col, "sum")
            )
        )

        employees_df = employees_df.copy()
        emp_branchid_col = next(
            (c for c in employees_df.columns if c.lower() == "branchid"),
            None
        )

        if emp_branchid_col:
            employees_df["_branchid"] = employees_df[emp_branchid_col].astype(str).str.strip().str.upper()
            employees_df = employees_df.merge(
                aggregated[["_branchid", "n_retornos", "monto_retornos"]],
                on="_branchid",
                how="left"
            )
            employees_df = employees_df.drop(columns=["_branchid"], errors="ignore")
            employees_df["n retornos"] = employees_df["n_retornos"].fillna(0)
            employees_df["monto retornos"] = employees_df["monto_retornos"].fillna(0)
            employees_df = employees_df.drop(columns=["n_retornos", "monto_retornos"], errors="ignore")
        else:
            employees_df["n retornos"] = 0
            employees_df["monto retornos"] = 0

        return employees_df

    def _calculate_sales_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["pago_cumplimiento_venta"] = result["cumplimiento venta"].apply(
            lambda x: self._get_threshold_payment(x, self.SALES_THRESHOLDS)
        )
        return result

    def _calculate_productivity_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["pago_productividad"] = result["cumplimiento productividad"].apply(
            lambda x: self._get_threshold_payment(x, self.PRODUCTIVITY_THRESHOLDS)
        )
        return result

    def _calculate_efficiency_payment(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["pago_eficiencia"] = result["cumplimiento eficiencia"].apply(
            lambda x: self._get_threshold_payment(x, self.EFFICIENCY_THRESHOLDS)
        )
        return result

    def _calculate_nps_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        result["cumplimiento_nps"] = result["resultado nps"].apply(
            lambda x: self._get_threshold_payment(x, self.NPS_THRESHOLDS)
        )
        return result

    def _calculate_returns_compliance(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        def calc_returns_compliance(row) -> float:
            n_retornos = row.get("n retornos", 0)
            monto = row.get("monto retornos", 0)

            if pd.isna(n_retornos) or pd.isna(monto):
                return 0

            if monto >= 4000000:
                return 0.00

            if n_retornos <= 1:
                return 1.00
            elif n_retornos == 2:
                return 0.50
            else:
                return 0.00

        result["cumplimiento_retornos"] = result.apply(calc_returns_compliance, axis=1)
        return result

    def _get_threshold_payment(self, value: float, thresholds: list) -> float:
        if pd.isna(value):
            return 0
        for lower, upper, payment in thresholds:
            if lower <= value <= upper:
                return payment
        return 0

    def _calculate_total_commission(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        base_payment = (
            result["pago_cumplimiento_venta"].fillna(0) +
            result["pago_productividad"].fillna(0) +
            result["pago_eficiencia"].fillna(0)
        )

        nps_factor = 1 + result["cumplimiento_nps"].fillna(0)
        returns_factor = result["cumplimiento_retornos"].fillna(0)

        result["commission"] = (base_payment * nps_factor * returns_factor).fillna(0)
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

    def _find_column(self, df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None


JefeTallerStrategy = WorkshopManagerStrategy
