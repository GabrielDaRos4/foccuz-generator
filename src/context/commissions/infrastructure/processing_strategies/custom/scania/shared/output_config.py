from dataclasses import dataclass


@dataclass(frozen=True)
class OutputConfig:
    column_rename_map: dict[str, str]
    output_columns: list[str]
    column_types: dict[str, str]

    def get_renamed_column(self, internal_name: str) -> str:
        return self.column_rename_map.get(internal_name, internal_name)

    def get_column_type(self, column_name: str) -> str:
        return self.column_types.get(column_name, "text")


MECHANIC_TECHNICIAN_OUTPUT = OutputConfig(
    column_rename_map={
        "productivity": "Productividad",
        "productivity_payment": "Pago Productividad",
        "efficiency": "Eficiencia",
        "efficiency_payment": "Pago Eficiencia",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    },
    output_columns=[
        "Productividad", "Pago Productividad",
        "Eficiencia", "Pago Eficiencia",
        "Días Trabajados", "Monto Final", "Comisión",
    ],
    column_types={
        "Productividad": "percentage",
        "Pago Productividad": "money",
        "Eficiencia": "percentage",
        "Pago Eficiencia": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    },
)


SERVICE_MANAGER_OUTPUT = OutputConfig(
    column_rename_map={
        "sales_compliance": "Cumplimiento Venta",
        "sales_compliance_payment": "Pago Cumplimiento Venta",
        "nps_result": "Resultado NPS",
        "nps_compliance_payment": "Pago NPS",
        "team_management_compliance": "Cumplimiento Gestión Equipo",
        "wip_factor": "Factor WIP",
        "ebit_factor": "Factor EBIT",
        "final_payment": "Pago Final",
        "guaranteed": "Garantizado",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    },
    output_columns=[
        "Cumplimiento Venta", "Pago Cumplimiento Venta",
        "Resultado NPS", "Pago NPS",
        "Cumplimiento Gestión Equipo", "Factor WIP", "Factor EBIT",
        "Pago Final", "Garantizado",
        "Días Trabajados", "Monto Final", "Comisión",
    ],
    column_types={
        "Cumplimiento Venta": "percentage",
        "Pago Cumplimiento Venta": "money",
        "Resultado NPS": "percentage",
        "Pago NPS": "money",
        "Cumplimiento Gestión Equipo": "percentage",
        "Factor WIP": "percentage",
        "Factor EBIT": "percentage",
        "Pago Final": "money",
        "Garantizado": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    },
)


GENERIC_COMPLIANCE_OUTPUT = OutputConfig(
    column_rename_map={
        "compliance": "Cumplimiento",
        "commission_payment": "Pago Cumplimiento",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    },
    output_columns=[
        "Cumplimiento", "Pago Cumplimiento",
        "Días Trabajados", "Monto Final", "Comisión",
    ],
    column_types={
        "Cumplimiento": "percentage",
        "Pago Cumplimiento": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    },
)


CLAIMS_ADVISOR_OUTPUT = OutputConfig(
    column_rename_map={
        "leadtime": "Lead Time",
        "leadtime_payment": "Pago Lead Time",
        "wip": "WIP",
        "wip_payment": "Pago WIP",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    },
    output_columns=[
        "Lead Time", "Pago Lead Time",
        "WIP", "Pago WIP",
        "Días Trabajados", "Monto Final", "Comisión",
    ],
    column_types={
        "Lead Time": "decimal",
        "Pago Lead Time": "money",
        "WIP": "percentage",
        "Pago WIP": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    },
)


PRESALES_OUTPUT = OutputConfig(
    column_rename_map={
        "configuration_count": "Configuraciones",
        "configuration_payment": "Pago Configuraciones",
        "guaranteed": "Garantizado",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    },
    output_columns=[
        "Configuraciones", "Pago Configuraciones",
        "Garantizado",
        "Días Trabajados", "Monto Final", "Comisión",
    ],
    column_types={
        "Configuraciones": "integer",
        "Pago Configuraciones": "money",
        "Garantizado": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    },
)


SALES_COMMISSION_OUTPUT = OutputConfig(
    column_rename_map={
        "total_sales": "Ventas Totales",
        "commission_percentage": "Porcentaje Comisión",
        "commission_amount": "Monto Comisión",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    },
    output_columns=[
        "Ventas Totales", "Porcentaje Comisión", "Monto Comisión",
        "Días Trabajados", "Monto Final", "Comisión",
    ],
    column_types={
        "Ventas Totales": "money",
        "Porcentaje Comisión": "percentage",
        "Monto Comisión": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    },
)


CLAIMS_WAREHOUSE_OUTPUT = OutputConfig(
    column_rename_map={
        "compliance": "Cumplimiento",
        "compliance_payment": "Pago Cumplimiento",
        "inventory_accuracy": "Exactitud Inventario",
        "inventory_payment": "Pago Inventario",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    },
    output_columns=[
        "Cumplimiento", "Pago Cumplimiento",
        "Exactitud Inventario", "Pago Inventario",
        "Días Trabajados", "Monto Final", "Comisión",
    ],
    column_types={
        "Cumplimiento": "percentage",
        "Pago Cumplimiento": "money",
        "Exactitud Inventario": "percentage",
        "Pago Inventario": "money",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    },
)


CWS_MANAGER_OUTPUT = OutputConfig(
    column_rename_map={
        "sales_compliance": "Cumplimiento Ventas",
        "sales_payment": "Pago Ventas",
        "efficiency": "Eficiencia",
        "efficiency_payment": "Pago Eficiencia",
        "productivity": "Productividad",
        "productivity_payment": "Pago Productividad",
        "team_factor": "Factor Equipo",
        "days_worked": "Días Trabajados",
        "final_amount": "Monto Final",
        "commission": "Comisión",
    },
    output_columns=[
        "Cumplimiento Ventas", "Pago Ventas",
        "Eficiencia", "Pago Eficiencia",
        "Productividad", "Pago Productividad",
        "Factor Equipo",
        "Días Trabajados", "Monto Final", "Comisión",
    ],
    column_types={
        "Cumplimiento Ventas": "percentage",
        "Pago Ventas": "money",
        "Eficiencia": "percentage",
        "Pago Eficiencia": "money",
        "Productividad": "percentage",
        "Pago Productividad": "money",
        "Factor Equipo": "percentage",
        "Días Trabajados": "integer",
        "Monto Final": "money",
        "Comisión": "money",
    },
)
