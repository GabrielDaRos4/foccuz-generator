from datetime import datetime

import pandas as pd

from .brand import Brand
from .consultant_bonus import ConsultantBonus


class OutputFormatter:

    COLUMN_TYPES = {
        "Fecha": "date",
        "Rep ID": "text",
        "ID Transaccion": "text",
        "Ventas Entregadas": "number",
        "Meta Ventas": "number",
        "Cumple Meta": "text",
        "Comision": "money"
    }

    OUTPUT_COLUMNS = [
        "Fecha", "Rep ID", "ID Transaccion", "Ventas Entregadas", "Meta Ventas",
        "Cumple Meta", "Comision"
    ]

    def format(
        self,
        bonuses: list[ConsultantBonus],
        brand: Brand,
        period: datetime
    ) -> pd.DataFrame:
        if not bonuses:
            return pd.DataFrame()

        rows = []
        for b in bonuses:
            rows.append({
                "Fecha": period.strftime("%Y-%m-%d"),
                "Rep ID": b.consultant_id,
                "ID Transaccion": f"{period.strftime('%Y-%m-%d')}_{brand.value}_{b.consultant_id}",
                "Ventas Entregadas": b.sales_count,
                "Meta Ventas": b.target,
                "Cumple Meta": "SI" if b.qualifies else "NO",
                "Comision": b.bonus
            })

        df = pd.DataFrame(rows)
        df = df[[c for c in self.OUTPUT_COLUMNS if c in df.columns]]
        df = df[df["Comision"] > 0]
        df.attrs["column_types"] = self.COLUMN_TYPES

        return df
