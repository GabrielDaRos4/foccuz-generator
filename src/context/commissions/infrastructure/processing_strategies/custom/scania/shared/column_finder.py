import pandas as pd


class ColumnFinder:

    @staticmethod
    def find_by_pattern(df: pd.DataFrame, *patterns: str) -> str | None:
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None

    @staticmethod
    def find_by_exact(df: pd.DataFrame, *names: str) -> str | None:
        for name in names:
            if name in df.columns:
                return name
            if name.lower() in [c.lower() for c in df.columns]:
                return next(c for c in df.columns if c.lower() == name.lower())
        return None

    @staticmethod
    def find_productivity_column(df: pd.DataFrame) -> str | None:
        return ColumnFinder.find_by_pattern(df, "productividad", "productivity")

    @staticmethod
    def find_efficiency_column(df: pd.DataFrame) -> str | None:
        return ColumnFinder.find_by_pattern(df, "eficiencia", "efficiency")

    @staticmethod
    def find_compliance_column(df: pd.DataFrame) -> str | None:
        return ColumnFinder.find_by_pattern(df, "cumplimiento", "compliance")

    @staticmethod
    def find_sales_column(df: pd.DataFrame) -> str | None:
        return ColumnFinder.find_by_pattern(
            df, "valor $", "valor_$", "precio venta", "monto", "ventas"
        )

    @staticmethod
    def find_days_column(df: pd.DataFrame) -> str | None:
        return ColumnFinder.find_by_pattern(
            df, "dias trabajados", "días trabajados", "days_worked", "dias"
        )

    @staticmethod
    def find_guaranteed_column(df: pd.DataFrame) -> str | None:
        return ColumnFinder.find_by_pattern(df, "garantizado", "guaranteed")

    @staticmethod
    def find_rut_column(df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if "rut" in col_lower and "cliente" not in col_lower:
                return col
        return None

    @staticmethod
    def find_branch_column(df: pd.DataFrame) -> str | None:
        return ColumnFinder.find_by_pattern(df, "branchid", "branch_id", "sucursal")

    @staticmethod
    def find_numeric_commission_column(df: pd.DataFrame) -> str | None:
        for col in df.columns:
            col_lower = col.lower()
            if col_lower == "comision" or col_lower == "comisión":
                if ColumnFinder._is_numeric_column(df, col):
                    return col

        for col in df.columns:
            col_lower = col.lower()
            if "comision" in col_lower or "comisión" in col_lower:
                if "cliente" not in col_lower:
                    if ColumnFinder._is_numeric_column(df, col):
                        return col

        return None

    @staticmethod
    def _is_numeric_column(df: pd.DataFrame, col: str) -> bool:
        values = pd.to_numeric(df[col], errors="coerce")
        return values.notna().any() and values.sum() > 0
