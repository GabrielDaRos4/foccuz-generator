import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent.parent.parent.parent.parent.parent.parent / "data" / "grupo_vanguardia"


class CommissionLookup:

    def __init__(self):
        self._acura_tables = self._load_json("comisiones_acura.json")
        self._honda_tables = self._load_json("comisiones_honda.json")
        self._missing_models = set()

    @staticmethod
    def _load_json(filename: str) -> dict:
        filepath = DATA_DIR / filename
        try:
            with open(filepath, encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Commission file not found: {filepath}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON {filepath}: {e}")
            return {}

    def lookup(
        self,
        brand: str,
        car_model: str,
        order_date: datetime,
        commission_type: str
    ) -> int | None:
        normalized_model = self._normalize_text(car_model)
        normalized_type = self._normalize_text(commission_type)

        if brand.upper() == "ACURA":
            table = self._get_acura_table(order_date)
        elif brand.upper() == "HONDA":
            table = self._get_honda_table(order_date)
        else:
            return None

        if not table:
            return None

        commission = self._find_commission(table, normalized_model, normalized_type)

        if commission is None:
            self._missing_models.add((brand, car_model, order_date.strftime("%Y-%m")))

        return commission

    def _get_acura_table(self, order_date: datetime) -> dict:
        if order_date >= datetime(2025, 5, 1) and order_date < datetime(2025, 9, 1):
            return self._acura_tables.get("mayo_agosto", {})
        elif order_date >= datetime(2025, 9, 1) and order_date < datetime(2025, 10, 1):
            return self._acura_tables.get("septiembre", {})
        elif order_date >= datetime(2025, 10, 1) and order_date < datetime(2025, 11, 1):
            return self._acura_tables.get("octubre", {})
        return {}

    def _get_honda_table(self, order_date: datetime) -> dict:
        if order_date >= datetime(2025, 5, 1) and order_date < datetime(2025, 6, 1):
            return self._honda_tables.get("mayo", {})
        elif order_date >= datetime(2025, 7, 1) and order_date < datetime(2025, 8, 1):
            return self._honda_tables.get("julio", {})
        elif order_date >= datetime(2025, 8, 1) and order_date < datetime(2025, 9, 1):
            return self._honda_tables.get("agosto", {})
        elif order_date >= datetime(2025, 9, 1) and order_date < datetime(2025, 10, 1):
            return self._honda_tables.get("septiembre", {})
        elif order_date >= datetime(2025, 10, 1) and order_date < datetime(2025, 11, 1):
            return self._honda_tables.get("octubre", {})
        elif order_date >= datetime(2025, 11, 1) and order_date < datetime(2025, 12, 1):
            return self._honda_tables.get("noviembre", {})
        return {}

    def _find_commission(self, table: dict, model: str, commission_type: str) -> int | None:
        for table_model, values in table.items():
            if self._normalize_text(table_model) == model:
                return values.get(commission_type)
        return None

    @staticmethod
    def _normalize_text(text: str) -> str:
        if not isinstance(text, str):
            return str(text)
        return (
            text.replace("–", "-")
            .replace("—", "-")
            .replace("\u00A0", " ")
            .strip()
            .upper()
        )

    def get_missing_models(self) -> list[tuple]:
        return list(self._missing_models)
