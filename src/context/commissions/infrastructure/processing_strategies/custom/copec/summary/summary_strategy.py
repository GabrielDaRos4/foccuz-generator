import logging
from datetime import datetime

import pandas as pd

from src.context.commissions.domain.ports import ProcessingStrategy

logger = logging.getLogger(__name__)

NEW_CLIENT_PLANS = [
    "PLAN_800",
    "PLAN_806",
    "PLAN_835",
    "PLAN_836",
    "PLAN_837",
    "PLAN_838",
    "PLAN_839",
]

LUBRICANTS_PLAN = "PLAN_842"

POA_PLANS = [
    "PLAN_786",
    "PLAN_856",
]

QUARTERLY_TEAM_PLAN = "PLAN_920"


SUMMARY_REP_ID = "EQUIPOVENTAS"


class CopecSummaryStrategy(ProcessingStrategy):

    def __init__(
        self,
        target_period: str = None,
        rep_id_filter: str = None,
    ):
        self._target_period = target_period
        self._rep_id_filter = rep_id_filter

    def calculate_commission(self, data: pd.DataFrame) -> pd.DataFrame:
        sources = data.attrs.get("sources", {})

        if not sources:
            logger.warning("No sources found for summary")
            return pd.DataFrame()

        self._employees_lookup = self._build_employees_lookup(sources.get("ejecutivos"))
        self._jefatura_lookup = self._build_jefatura_lookup(sources.get("ejecutivos"))
        self._team_bonus_lookup = self._build_team_bonus_lookup(sources.get(QUARTERLY_TEAM_PLAN))

        all_executives = self._collect_all_executives(sources)

        if not all_executives:
            logger.warning("No executives found")
            return pd.DataFrame()

        results = []
        for rep_id, name in all_executives.items():
            executive_name = self._get_employee_name(rep_id) or name
            row = self._build_executive_row(rep_id, executive_name, sources)
            results.append(row)

        result_df = pd.DataFrame(results)

        if self._rep_id_filter:
            result_df = result_df[result_df["Rep ID"] == self._rep_id_filter]

        result_df = self._format_output(result_df)

        return result_df

    def _collect_all_executives(self, sources: dict) -> dict[str, str]:
        executives = {}

        for plan_name, plan_df in sources.items():
            if plan_name == "ejecutivos":
                continue

            if plan_df is None or plan_df.empty:
                continue

            logger.debug(f"{plan_name} columns: {list(plan_df.columns)}")

            rep_id_col = self._find_rep_id_column(plan_df)
            name_col = self._find_name_column(plan_df)

            if rep_id_col is None:
                logger.debug(f"{plan_name}: Rep ID column not found")
                continue

            for _, row in plan_df.iterrows():
                rep_id = str(row.get(rep_id_col, "")).strip()
                if not rep_id or rep_id.lower() in ("nan", "none", ""):
                    continue

                if rep_id not in executives:
                    if name_col:
                        name = str(row.get(name_col, "")).strip()
                        executives[rep_id] = name if name and name.lower() not in ("nan", "none") else rep_id
                    else:
                        executives[rep_id] = rep_id

        logger.debug(f"Found {len(executives)} unique executives")
        return executives

    @staticmethod
    def _find_rep_id_column(df: pd.DataFrame) -> str | None:
        possible_names = ["Rep ID", "rep_id", "REP ID", "Rut", "RUT"]
        for col in df.columns:
            if col in possible_names:
                return col
        return None

    @staticmethod
    def _find_name_column(df: pd.DataFrame) -> str | None:
        possible_names = ["Ejecutivo", "ejecutivo", "Nombre", "nombre", "Vendedor", "vendedor"]
        for col in df.columns:
            if col in possible_names:
                return col
        return None

    def _build_employees_lookup(self, employees_df: pd.DataFrame) -> dict[str, str]:
        lookup = {}
        if employees_df is None or employees_df.empty:
            return lookup

        rut_col = None
        name_col = None
        for col in employees_df.columns:
            if col.lower() == "rut":
                rut_col = col
            elif col.lower() == "nombre":
                name_col = col

        if rut_col is None or name_col is None:
            logger.warning(f"Employees CSV missing Rut or Nombre column. Columns: {list(employees_df.columns)}")
            return lookup

        for _, row in employees_df.iterrows():
            rut = str(row.get(rut_col, "")).strip()
            name = str(row.get(name_col, "")).strip()
            if rut and name:
                normalized_rut = self._normalize_rut(rut)
                lookup[normalized_rut] = name

        logger.debug(f"Built employees lookup with {len(lookup)} entries")
        return lookup

    @staticmethod
    def _normalize_rut(rut: str) -> str:
        return rut.replace(".", "").replace(" ", "").strip()

    def _get_employee_name(self, rep_id: str) -> str | None:
        normalized = self._normalize_rut(rep_id)
        return self._employees_lookup.get(normalized)

    def _build_jefatura_lookup(self, employees_df: pd.DataFrame) -> dict[str, str]:
        lookup = {}
        if employees_df is None or employees_df.empty:
            return lookup

        rut_col = None
        jefatura_col = None
        for col in employees_df.columns:
            if col.lower() == "rut":
                rut_col = col
            elif col.lower() == "jefatura":
                jefatura_col = col

        if rut_col is None or jefatura_col is None:
            return lookup

        for _, row in employees_df.iterrows():
            rut = str(row.get(rut_col, "")).strip()
            jefatura = str(row.get(jefatura_col, "")).strip()
            if rut and jefatura and jefatura.lower() not in ("nan", "none", ""):
                normalized_rut = self._normalize_rut(rut)
                lookup[normalized_rut] = jefatura

        return lookup

    def _build_team_bonus_lookup(self, team_df: pd.DataFrame) -> dict[str, float]:
        lookup = {}
        if team_df is None or team_df.empty:
            return lookup

        equipo_col = None
        bono_col = None
        for col in team_df.columns:
            if col.lower() == "equipo":
                equipo_col = col
            elif col.lower() in ("bono", "comision"):
                bono_col = col

        if equipo_col is None or bono_col is None:
            logger.warning(f"Team bonus data missing Equipo or Bono column. Columns: {list(team_df.columns)}")
            return lookup

        for _, row in team_df.iterrows():
            equipo = str(row.get(equipo_col, "")).strip()
            bono = self._parse_money_value(row.get(bono_col))
            if equipo and equipo.lower() not in ("nan", "none", ""):
                lookup[equipo] = bono

        logger.info(f"Built team bonus lookup with {len(lookup)} teams")
        return lookup

    def _get_quarterly_team_bonus(self, rep_id: str) -> float:
        normalized_rut = self._normalize_rut(rep_id)
        jefatura = self._jefatura_lookup.get(normalized_rut)
        if not jefatura:
            return 0.0
        return self._team_bonus_lookup.get(jefatura, 0.0)

    def _build_executive_row(
        self,
        rep_id: str,
        name: str,
        sources: dict
    ) -> dict:
        new_client_commission = self._sum_commissions_for_rep(
            rep_id, sources, NEW_CLIENT_PLANS
        )

        lubricants_commission = self._sum_commissions_for_rep(
            rep_id, sources, [LUBRICANTS_PLAN]
        )

        poa_bonus = self._sum_commissions_for_rep(rep_id, sources, POA_PLANS)

        quarterly_team_bonus = self._get_quarterly_team_bonus(rep_id)

        total = new_client_commission + lubricants_commission + poa_bonus + quarterly_team_bonus

        logger.debug(
            f"Executive {rep_id}: new_clients={new_client_commission}, "
            f"lubricants={lubricants_commission}, poa={poa_bonus}, "
            f"quarterly_team={quarterly_team_bonus}, total={total}"
        )

        period = self._extract_period()

        return {
            "Fecha": period.strftime("%Y-%m-%d"),
            "Rep ID": SUMMARY_REP_ID,
            "ID Transaccion": f"RESUMEN_{rep_id}_{period.strftime('%Y%m')}",
            "Rut": rep_id,
            "Ejecutivo": name,
            "Comision Clientes Nuevos": new_client_commission,
            "Comision Lubricantes": lubricants_commission,
            "Bono Cumplimiento POA": poa_bonus,
            "Bono Equipo Trimestral": quarterly_team_bonus,
            "Total": total,
            "Comision": total,
        }

    def _sum_commissions_for_rep(
        self,
        rep_id: str,
        sources: dict,
        plan_names: list[str]
    ) -> float:
        total = 0.0

        for plan_name in plan_names:
            plan_df = sources.get(plan_name)
            if plan_df is None or plan_df.empty:
                continue

            rep_id_col = self._find_rep_id_column(plan_df)
            if rep_id_col is None:
                continue

            commission_col = self._find_commission_column(plan_df)
            if commission_col is None:
                continue

            rep_rows = plan_df[plan_df[rep_id_col].astype(str).str.strip() == rep_id]

            plan_total = 0.0
            if not rep_rows.empty:
                for _, row in rep_rows.iterrows():
                    value = self._parse_money_value(row.get(commission_col))
                    plan_total += value
                logger.info(f"Rep {rep_id} - {plan_name}: {len(rep_rows)} rows, commission={plan_total}")
            total += plan_total

        return total

    @staticmethod
    def _parse_money_value(value) -> float:
        if value is None or pd.isna(value):
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        str_value = str(value).strip()
        if not str_value or str_value == "-":
            return 0.0
        cleaned = str_value.replace("$", "").replace(" ", "").strip()
        if "," in cleaned and "." in cleaned:
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")
        elif "," in cleaned:
            cleaned = cleaned.replace(",", "")
        elif "." in cleaned:
            parts = cleaned.split(".")
            if len(parts) == 2 and len(parts[1]) == 3:
                cleaned = cleaned.replace(".", "")
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return 0.0

    def _sum_bonus_for_rep(
        self,
        rep_id: str,
        sources: dict,
        plan_names: list[str]
    ) -> float:
        total = 0.0

        for plan_name in plan_names:
            plan_df = sources.get(plan_name)
            if plan_df is None or plan_df.empty:
                logger.debug(f"POA {plan_name}: no data")
                continue

            rep_id_col = self._find_rep_id_column(plan_df)
            if rep_id_col is None:
                logger.debug(f"POA {plan_name}: no rep_id column. Columns: {list(plan_df.columns)}")
                continue

            bonus_col = self._find_bonus_column(plan_df)
            if bonus_col is None:
                logger.debug(f"POA {plan_name}: no bonus column. Columns: {list(plan_df.columns)}")
                continue

            rep_rows = plan_df[plan_df[rep_id_col].astype(str).str.strip() == rep_id]

            plan_total = 0.0
            if not rep_rows.empty:
                for _, row in rep_rows.iterrows():
                    value = self._parse_money_value(row.get(bonus_col))
                    plan_total += value
                logger.info(f"Rep {rep_id} - {plan_name} (POA): {len(rep_rows)} rows, bonus={plan_total}")
            total += plan_total

        return total

    @staticmethod
    def _find_commission_column(df: pd.DataFrame) -> str | None:
        possible_names = ["Comision", "comision", "COMISION", "Commission"]
        for col in df.columns:
            if col in possible_names:
                return col
        return None

    @staticmethod
    def _find_bonus_column(df: pd.DataFrame) -> str | None:
        possible_names = ["Bono", "bono", "BONO", "Bonus"]
        for col in df.columns:
            if col in possible_names:
                return col
        return None

    def _extract_period(self) -> datetime:
        if self._target_period:
            try:
                if isinstance(self._target_period, datetime):
                    return self._target_period
                return datetime.strptime(str(self._target_period), "%Y-%m-%d")
            except (ValueError, AttributeError):
                pass
        return datetime.now().replace(day=1)

    @staticmethod
    def _format_output(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        output_columns = [
            "Fecha",
            "Rep ID",
            "ID Transaccion",
            "Rut",
            "Ejecutivo",
            "Comision Clientes Nuevos",
            "Comision Lubricantes",
            "Bono Cumplimiento POA",
            "Bono Equipo Trimestral",
            "Total",
            "Comision",
        ]

        existing_cols = [c for c in output_columns if c in df.columns]
        result = df[existing_cols].copy()

        money_cols = [
            "Comision Clientes Nuevos",
            "Comision Lubricantes",
            "Bono Cumplimiento POA",
            "Bono Equipo Trimestral",
            "Total",
            "Comision",
        ]
        for col in money_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0).astype(int)

        result = result.sort_values("Ejecutivo").reset_index(drop=True)

        result.attrs["column_types"] = {
            "Fecha": "date",
            "Rep ID": "text",
            "ID Transaccion": "text",
            "Rut": "text",
            "Ejecutivo": "text",
            "Comision Clientes Nuevos": "money",
            "Comision Lubricantes": "money",
            "Bono Cumplimiento POA": "money",
            "Bono Equipo Trimestral": "money",
            "Total": "money",
            "Comision": "money",
        }

        return result
