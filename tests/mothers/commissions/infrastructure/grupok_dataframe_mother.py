import pandas as pd


class GrupoKDataFrameMother:

    @staticmethod
    def store_manager_employees(count: int = 2) -> pd.DataFrame:
        employees = [
            {
                'id': 1001,
                'rut': '13.215.149-0',
                'full_name': 'Katherine Alday',
                'current_job': {
                    'role': {'name': 'Jefe de Tienda'},
                    'custom_attributes': {'Lugar de trabajo (Sucursal)': 'Iquique'}
                }
            },
            {
                'id': 1002,
                'rut': '17.190.840-K',
                'full_name': 'Camila Alvarez',
                'current_job': {
                    'role': {'name': 'Jefe de Tienda'},
                    'custom_attributes': {'Lugar de trabajo (Sucursal)': 'Antofagasta'}
                }
            },
        ]
        return pd.DataFrame(employees[:count])

    @staticmethod
    def mixed_role_employees() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'id': 1001,
                'rut': '13.215.149-0',
                'full_name': 'Katherine Alday',
                'current_job': {
                    'role': {'name': 'Jefe de Tienda'},
                    'custom_attributes': {'Lugar de trabajo (Sucursal)': 'Iquique'}
                }
            },
            {
                'id': 1002,
                'rut': '17.190.840-K',
                'full_name': 'Camila Alvarez',
                'current_job': {
                    'role': {'name': 'Jefe de Tienda'},
                    'custom_attributes': {'Lugar de trabajo (Sucursal)': 'Antofagasta'}
                }
            },
            {
                'id': 1003,
                'rut': '18.234.567-8',
                'full_name': 'Juan Lopez',
                'current_job': {
                    'role': {'name': 'Asesor de venta'},
                    'custom_attributes': {'Lugar de trabajo (Sucursal)': 'Iquique'}
                }
            }
        ])

    @staticmethod
    def manager_without_store() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'id': 1001,
                'rut': '13.215.149-0',
                'full_name': 'Katherine Alday',
                'current_job': {
                    'role': {'name': 'Jefe de Tienda'},
                    'custom_attributes': {}
                }
            }
        ])

    @staticmethod
    def sales_with_multiple_stores() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'tipo_venta': 'RTL',
                'sucursal': 'Iquique',
                'vendedor': 'Vendedor 1',
                'rut_vendedor': '19000000-1',
                'fecha': '2025-11-15',
                'razon_social': 'Cliente A',
                'ndoc': 'F001',
                'monto_neto': 25000000
            },
            {
                'tipo_venta': 'RTL',
                'sucursal': 'Iquique',
                'vendedor': 'Vendedor 2',
                'rut_vendedor': '19000000-2',
                'fecha': '2025-11-20',
                'razon_social': 'Cliente B',
                'ndoc': 'F002',
                'monto_neto': 20000000
            },
            {
                'tipo_venta': 'RTL',
                'sucursal': 'Antofagasta',
                'vendedor': 'Vendedor 3',
                'rut_vendedor': '19000000-3',
                'fecha': '2025-11-10',
                'razon_social': 'Cliente C',
                'ndoc': 'F003',
                'monto_neto': 60000000
            },
            {
                'tipo_venta': 'RTL',
                'sucursal': 'Santiago',
                'vendedor': 'Vendedor 4',
                'rut_vendedor': '19000000-4',
                'fecha': '2025-11-12',
                'razon_social': 'Cliente D',
                'ndoc': 'F004',
                'monto_neto': 50000000
            }
        ])

    @staticmethod
    def sales_with_different_periods() -> pd.DataFrame:
        return pd.DataFrame([
            {
                'sucursal': 'Iquique',
                'vendedor': 'Vendedor 1',
                'rut_vendedor': '19000000-1',
                'fecha': '2025-11-15',
                'ndoc': 'F001',
                'monto_neto': 25000000
            },
            {
                'sucursal': 'Iquique',
                'vendedor': 'Vendedor 2',
                'rut_vendedor': '19000000-2',
                'fecha': '2025-10-15',
                'ndoc': 'F002',
                'monto_neto': 20000000
            }
        ])

    @staticmethod
    def commission_tiers() -> pd.DataFrame:
        return pd.DataFrame([
            {'n': 1, 'desde': 0, 'hasta': 40000000, 'comision_bruta': 700000},
            {'n': 2, 'desde': 40000001, 'hasta': 56000000, 'comision_bruta': 820000},
            {'n': 3, 'desde': 56000001, 'hasta': 72000000, 'comision_bruta': 940000},
            {'n': 4, 'desde': 72000001, 'hasta': None, 'comision_bruta': 1060000}
        ])

    @staticmethod
    def empty() -> pd.DataFrame:
        return pd.DataFrame()

    @staticmethod
    def create_data_with_attrs(
        employees: pd.DataFrame,
        sales: pd.DataFrame,
        tiers: pd.DataFrame
    ) -> pd.DataFrame:
        df = pd.DataFrame({'_placeholder': [1]})
        df.attrs['employees'] = employees
        df.attrs['sales'] = sales
        df.attrs['commission_tiers'] = tiers
        return df
