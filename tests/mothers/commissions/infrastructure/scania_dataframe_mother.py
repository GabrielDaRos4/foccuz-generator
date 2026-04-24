import pandas as pd


class ScaniaDataFrameMother:

    @staticmethod
    def employees(rows: int = 3) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'id empleado': 1001,
                'rut': '16.766.611-6',
                'full_name': 'Pedro Montecinos',
                'cargo': 'Jefe de Servicio',
                'branch': 'Santiago',
                'branchid': 'SCL001',
                'days_worked': 30
            },
            {
                'id empleado': 1002,
                'rut': '17.123.456-7',
                'full_name': 'Maria Garcia',
                'cargo': 'Operario Bodega Siniestros',
                'branch': 'Valparaiso',
                'branchid': 'VLP001',
                'days_worked': 20
            },
            {
                'id empleado': 1003,
                'rut': '18.234.567-8',
                'full_name': 'Juan Lopez',
                'cargo': 'Jefe de Taller',
                'branch': 'Concepcion',
                'branchid': 'CCP001',
                'days_worked': 30
            }
        ][:rows])

    @staticmethod
    def plan_data_with_rut() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'rut': '16766611-6',
                'cumplimiento venta': 115,
                'resultado_nps': 95,
                'tamano': 'grande'
            },
            {
                'rut': '17123456-7',
                'cumplimiento venta': 105,
                'resultado_nps': 88,
                'tamano': 'mediana'
            },
            {
                'rut': '18234567-8',
                'cumplimiento venta': 120,
                'resultado_nps': 100,
                'tamano': 'pequeña'
            }
        ])

    @staticmethod
    def plan_data_with_branch() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'branchid': 'SCL001',
                'cumplimiento': 110,
                'meta': 100000
            },
            {
                'branchid': 'VLP001',
                'cumplimiento': 95,
                'meta': 80000
            },
            {
                'branchid': 'CCP001',
                'cumplimiento': 105,
                'meta': 90000
            }
        ])

    @staticmethod
    def service_manager_data() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'id empleado': 1001,
                'rut': '16.766.611-6',
                'full_name': 'Pedro Montecinos',
                'cargo': 'jefe de servicio',
                'branch': 'Santiago',
                'branchid': 'SCL001',
                'days_worked': 30,
                'cumplimiento venta': 115,
                'resultado_nps': 0.95,
                'tamano': 'grande',
                'accidentabilidad': 1.0,
                'rotacion': 0.01,
                'ausentismo': 0.035,
                'wip': 0.20,
                'ebit': 0.12
            }
        ])

    @staticmethod
    def generic_compliance_data() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'id empleado': 1001,
                'rut': '16.766.611-6',
                'cargo': 'operario bodega siniestros',
                'branch': 'Santiago',
                'branchid': 'SCL001',
                'days_worked': 30,
                'cumplimiento': 110
            },
            {
                'id empleado': 1002,
                'rut': '17.123.456-7',
                'cargo': 'operario bodega siniestros',
                'branch': 'Valparaiso',
                'branchid': 'VLP001',
                'days_worked': 20,
                'cumplimiento': 95
            }
        ])

    @staticmethod
    def cws_manager_data() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'id empleado': 1001,
                'rut': '16.766.611-6',
                'cargo': 'jefe cws',
                'branch': 'Santiago',
                'branchid': 'SCL001',
                'days_worked': 30,
                '% ajuste de inventario': 0.005,
                'inventario rotativo pendiente': 0,
                'arribo fuera de plazo': 0,
                'real ubicacion de repuestos': 95,
                'meta ubicacion de repuestos': 100,
                'real disponibilidad de flota': 98,
                'meta disponibilidad de flota': 95,
                'pago ot abiertas': 200000
            }
        ])

    @staticmethod
    def empty() -> pd.DataFrame:
        return pd.DataFrame()
