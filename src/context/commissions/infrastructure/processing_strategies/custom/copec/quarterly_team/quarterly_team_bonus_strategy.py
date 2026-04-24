import logging
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from src.context.commissions.domain.ports import ProcessingStrategy

logger = logging.getLogger(__name__)

TEAM_BONUS_AMOUNT = 6_000_000
REP_ID_VALUE = "EQUIPOVENTAS"


class CopecQuarterlyTeamBonusStrategy(ProcessingStrategy):

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
            logger.warning("No sources found for quarterly team bonus")
            return pd.DataFrame()

        employees_df = sources.get("ejecutivos")
        if employees_df is None or employees_df.empty:
            logger.warning("No employees data found")
            return pd.DataFrame()

        jefatura_lookup = self._build_jefatura_lookup(employees_df)
        if not jefatura_lookup:
            logger.warning("Could not build jefatura lookup")
            return pd.DataFrame()

        poa_df = sources.get("POA_RESUMEN")
        if poa_df is None:
            poa_df = sources.get("poa_resumen")
        if poa_df is None or poa_df.empty:
            logger.warning("No POA data found")
            return pd.DataFrame()

        quarter_months = self._get_quarter_months()
        logger.info(f"Processing quarter: {[m.strftime('%Y-%m') for m in quarter_months]}")

        team_data = self._aggregate_team_data(
            sources, jefatura_lookup, poa_df, quarter_months
        )

        if not team_data:
            logger.warning("No team data aggregated")
            return pd.DataFrame()

        results = self._build_results(team_data, quarter_months)

        result_df = pd.DataFrame(results)

        if self._rep_id_filter:
            result_df = result_df[result_df["Equipo"] == self._rep_id_filter]

        result_df = self._format_output(result_df, quarter_months)

        return result_df

    def _build_jefatura_lookup(self, employees_df: pd.DataFrame) -> dict[str, str]:
        lookup = {}

        rut_col = None
        jefatura_col = None
        for col in employees_df.columns:
            if col.lower() == "rut":
                rut_col = col
            elif col.lower() == "jefatura":
                jefatura_col = col

        if rut_col is None or jefatura_col is None:
            logger.warning(f"Missing columns. Columns: {list(employees_df.columns)}")
            return lookup

        for _, row in employees_df.iterrows():
            rut = str(row.get(rut_col, "")).strip()
            jefatura = str(row.get(jefatura_col, "")).strip()
            if rut and jefatura and jefatura.lower() not in ("nan", "none", ""):
                normalized_rut = self._normalize_rut(rut)
                lookup[normalized_rut] = jefatura

        logger.info(f"Built jefatura lookup with {len(lookup)} entries")
        unique_jefaturas = set(lookup.values())
        logger.info(f"Teams found: {unique_jefaturas}")
        return lookup

    @staticmethod
    def _normalize_rut(rut: str) -> str:
        return rut.replace(".", "").replace(" ", "").strip()

    def _get_quarter_months(self) -> list[datetime]:
        current_period = self._extract_period()
        months = []
        for i in range(2, -1, -1):
            month = current_period - relativedelta(months=i)
            months.append(month.replace(day=1))
        return months

    def _extract_period(self) -> datetime:
        if self._target_period:
            try:
                if isinstance(self._target_period, datetime):
                    return self._target_period
                return datetime.strptime(str(self._target_period), "%Y-%m-%d")
            except (ValueError, AttributeError):
                pass
        return datetime.now().replace(day=1)

    def _aggregate_team_data(
        self,
        sources: dict,
        jefatura_lookup: dict[str, str],
        poa_df: pd.DataFrame,
        quarter_months: list[datetime]
    ) -> dict[str, dict]:
        team_data = {}

        volume_sources = {
            "TCT_TAE": sources.get("TCT_TAE"),
            "CUPON_ELECTRONICO": sources.get("CUPON_ELECTRONICO"),
            "APP_COPEC": sources.get("APP_COPEC"),
            "BLUEMAX": sources.get("BLUEMAX"),
            "TCT_PREMIUM": sources.get("TCT_PREMIUM"),
            "LUBRICANTES": sources.get("LUBRICANTES"),
        }

        for team in set(jefatura_lookup.values()):
            team_data[team] = {
                "team_name": team,
                "members": [],
                "months": {},
                "total_real": 0.0,
                "total_poa": 0.0,
                "months_compliant": 0,
            }

        for rut, team in jefatura_lookup.items():
            if team in team_data:
                team_data[team]["members"].append(rut)

        for month in quarter_months:
            month_key = month.strftime("%Y-%m")
            for _, data in team_data.items():
                data["months"][month_key] = {
                    "real": 0.0,
                    "poa": 0.0,
                    "compliant": False,
                }

        poa_by_rut_month = self._build_poa_lookup(poa_df, quarter_months)

        for _, data in team_data.items():
            for month in quarter_months:
                month_key = month.strftime("%Y-%m")
                month_real = 0.0
                month_poa = 0.0

                for rut in data["members"]:
                    rep_poa = poa_by_rut_month.get(rut, {}).get(month_key, 0.0)
                    month_poa += rep_poa

                    rep_real = self._calculate_rep_volume(
                        volume_sources, rut, month
                    )
                    month_real += rep_real

                data["months"][month_key]["real"] = month_real
                data["months"][month_key]["poa"] = month_poa

                if month_poa > 0 and month_real >= month_poa:
                    data["months"][month_key]["compliant"] = True
                    data["months_compliant"] += 1
                elif month_poa == 0:
                    data["months"][month_key]["compliant"] = True
                    data["months_compliant"] += 1

                data["total_real"] += month_real
                data["total_poa"] += month_poa

        return team_data

    @staticmethod
    def _build_poa_lookup(
        poa_df: pd.DataFrame,
        quarter_months: list[datetime]
    ) -> dict[str, dict[str, float]]:
        lookup = {}

        if poa_df is None or poa_df.empty:
            return lookup

        poa_df_copy = poa_df.copy()

        rut_col = None
        for col in poa_df_copy.columns:
            if str(col).lower() == "rut":
                rut_col = col
                break

        if rut_col is None:
            logger.warning("POA data has no Rut column")
            return lookup

        if rut_col in poa_df_copy.columns:
            poa_df_copy["rut_sanitized"] = (
                poa_df_copy[rut_col].astype(str).str.replace(".", "", regex=False)
            )

        period_columns = {}
        for col in poa_df_copy.columns:
            if isinstance(col, datetime):
                month_key = col.strftime("%Y-%m")
                if any(m.strftime("%Y-%m") == month_key for m in quarter_months):
                    period_columns[month_key] = col

        logger.info(f"POA period columns found: {list(period_columns.keys())}")

        for _, row in poa_df_copy.iterrows():
            rut = row.get("rut_sanitized", "")
            if not rut or rut.lower() in ("nan", "none"):
                continue

            if rut not in lookup:
                lookup[rut] = {}

            for month_key, col in period_columns.items():
                value = row.get(col, 0)
                if pd.notna(value):
                    try:
                        lookup[rut][month_key] = lookup[rut].get(month_key, 0) + float(value)
                    except (ValueError, TypeError):
                        pass

        return lookup

    @staticmethod
    def _calculate_rep_volume(
        sources: dict,
        rep_id: str,
        month: datetime
    ) -> float:
        total_volume = 0.0
        rep_id_base = rep_id.split("-")[0] if "-" in rep_id else rep_id

        for source_name, source_df in sources.items():
            if source_df is None or source_df.empty:
                continue

            df = source_df.copy()
            df.columns = df.columns.str.lower().str.strip()

            column_mappings = {
                "vol. 2025": "volumen",
                "rut": "rut_ejecutivo",
            }
            df = df.rename(columns=column_mappings)

            if "rut_ejecutivo" not in df.columns:
                continue

            df_rut = df["rut_ejecutivo"].astype(str).str.replace(".", "", regex=False).str.strip()

            if "dv_ejecutivo" in df.columns:
                df_dv = df["dv_ejecutivo"].astype(str).str.strip()
                df_full_rut = df_rut + "-" + df_dv
                mask = (df_full_rut == rep_id) | (df_rut == rep_id_base)
            else:
                mask = (df_rut == rep_id) | (df_rut == rep_id_base)

            filtered_df = df[mask]

            if "anio" in filtered_df.columns and "mes" in filtered_df.columns:
                filtered_df = filtered_df[
                    (filtered_df["anio"].astype(int) == month.year) &
                    (filtered_df["mes"].astype(int) == month.month)
                ]

            volume_col = None
            for col_name in ["volumen", "volumen_lts", "volumen_tct_premium"]:
                if col_name in filtered_df.columns:
                    volume_col = col_name
                    break

            if volume_col:
                volume = pd.to_numeric(filtered_df[volume_col], errors="coerce").fillna(0).sum()
                if source_name in ("TCT_TAE", "CUPON_ELECTRONICO", "APP_COPEC", "BLUEMAX"):
                    volume = volume / 1000.0
                total_volume += volume

        return total_volume

    def _build_results(
        self,
        team_data: dict[str, dict],
        quarter_months: list[datetime]
    ) -> list[dict]:
        results = []
        period = self._extract_period()

        for team_name, data in team_data.items():
            is_compliant = data["months_compliant"] >= 3
            bonus = TEAM_BONUS_AMOUNT if is_compliant else 0

            row = {
                "Fecha": period.strftime("%Y-%m-%d"),
                "Rep ID": REP_ID_VALUE,
                "ID Transaccion": f"EQUIPO_Q_{team_name}_{period.strftime('%Y%m')}",
                "Equipo": team_name,
                "Miembros": len(data["members"]),
            }

            for month in quarter_months:
                month_key = month.strftime("%Y-%m")
                month_data = data["months"].get(month_key, {})
                month_name = self._get_month_name(month)

                row[f"Real {month_name}"] = round(month_data.get("real", 0), 2)
                row[f"POA {month_name}"] = round(month_data.get("poa", 0), 2)
                row[f"Cumple {month_name}"] = "Si" if month_data.get("compliant") else "No"

            row["Real Total Q"] = round(data["total_real"], 2)
            row["POA Total Q"] = round(data["total_poa"], 2)
            row["Meses Cumplidos"] = data["months_compliant"]
            row["Cumple Trimestre"] = "Si" if is_compliant else "No"
            row["Bono"] = bonus
            row["Comision"] = bonus

            results.append(row)

        return results

    @staticmethod
    def _get_month_name(month: datetime) -> str:
        spanish_months = {
            1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr", 5: "May", 6: "Jun",
            7: "Jul", 8: "Ago", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
        }
        return spanish_months.get(month.month, month.strftime("%b"))

    def _format_output(
        self,
        df: pd.DataFrame,
        quarter_months: list[datetime]
    ) -> pd.DataFrame:
        if df.empty:
            return df

        output_columns = [
            "Fecha",
            "Rep ID",
            "ID Transaccion",
            "Equipo",
            "Miembros",
        ]

        for month in quarter_months:
            month_name = self._get_month_name(month)
            output_columns.extend([
                f"Real {month_name}",
                f"POA {month_name}",
                f"Cumple {month_name}",
            ])

        output_columns.extend([
            "Real Total Q",
            "POA Total Q",
            "Meses Cumplidos",
            "Cumple Trimestre",
            "Bono",
            "Comision",
        ])

        existing_cols = [c for c in output_columns if c in df.columns]
        result = df[existing_cols].copy()

        result = result.sort_values("Equipo").reset_index(drop=True)

        column_types = {
            "Fecha": "date",
            "Rep ID": "text",
            "ID Transaccion": "text",
            "Equipo": "text",
            "Miembros": "integer",
            "Real Total Q": "number",
            "POA Total Q": "number",
            "Meses Cumplidos": "integer",
            "Cumple Trimestre": "text",
            "Bono": "money",
            "Comision": "money",
        }

        for month in quarter_months:
            month_name = self._get_month_name(month)
            column_types[f"Real {month_name}"] = "number"
            column_types[f"POA {month_name}"] = "number"
            column_types[f"Cumple {month_name}"] = "text"

        result.attrs["column_types"] = column_types

        return result

    def get_column_types(self) -> dict[str, str]:
        return {}
