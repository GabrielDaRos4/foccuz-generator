import pandas as pd


class DataFrameMother:

    @staticmethod
    def copec_sales(rows: int = 3) -> pd.DataFrame:
        return pd.DataFrame({
            'producto': ['TCT'] * rows,
            'ejecutivo': ['0001000206'] * rows,
            'rut_ejecutivo': ['12345678'] * rows,
            'dv_ejecutivo': ['9'] * rows,
            'rut_cliente': [f'{10000000 + i}' for i in range(rows)],
            'dv_cliente': [str(i % 10) for i in range(rows)],
            'volumen': [100.0 + i * 10 for i in range(rows)],
            'descuento': [50.0 + i * 5 for i in range(rows)],
            'anio': ['2025'] * rows,
            'mes': ['10'] * rows
        })

    @staticmethod
    def copec_sales_with_historical() -> pd.DataFrame:
        df = DataFrameMother.copec_sales(3)
        df.attrs['ventas_historicas'] = []
        df.attrs['empleados'] = None
        return df

    @staticmethod
    def grupo_vanguardia_honda_sales(rows: int = 5) -> pd.DataFrame:
        return pd.DataFrame({
            'Model': ['CR-V', 'CR-V', 'HR-V', 'Civic', 'Accord'][:rows],
            'Status': ['Entregado'] * rows,
            'Delivery_Date': ['2024-12-01'] * rows,
            'IdConsultant': [1, 1, 1, 2, 2][:rows],
            'Consultant_Name': ['Juan', 'Juan', 'Juan', 'Maria', 'Maria'][:rows],
            'Consultant_Mail': ['juan@test.com', 'juan@test.com', 'juan@test.com',
                               'maria@test.com', 'maria@test.com'][:rows],
            'Agency': ['HONDA COLIMA'] * rows
        })

    @staticmethod
    def grupo_vanguardia_acura_sales(rows: int = 4) -> pd.DataFrame:
        return pd.DataFrame({
            'Model': ['RDX', 'RDX', 'MDX', 'TLX'][:rows],
            'Status': ['Entregado'] * rows,
            'Delivery_Date': ['2024-12-01'] * rows,
            'IdConsultant': [1, 1, 2, 2][:rows],
            'Consultant_Name': ['Juan', 'Juan', 'Maria', 'Maria'][:rows],
            'Consultant_Mail': ['juan@test.com', 'juan@test.com',
                               'maria@test.com', 'maria@test.com'][:rows],
            'Agency': ['HONDA COLIMA'] * rows
        })

    @staticmethod
    def tiered_commission_input() -> pd.DataFrame:
        return pd.DataFrame({
            'employee_id': ['EMP001', 'EMP002', 'EMP003'],
            'employee_name': ['Low Seller', 'Mid Seller', 'Top Seller'],
            'ventas': [5000, 25000, 100000]
        })

    @staticmethod
    def empty() -> pd.DataFrame:
        return pd.DataFrame()

    @staticmethod
    def with_columns(**columns) -> pd.DataFrame:
        return pd.DataFrame(columns)

    @staticmethod
    def consultants() -> pd.DataFrame:
        return pd.DataFrame({
            'IdConsultant': [1, 2, 3],
            'Consultant_Name': ['Juan', 'Maria', 'Pedro'],
            'Consultant_Mail': ['juan@test.com', 'maria@test.com', 'pedro@test.com'],
            'Agency': ['HONDA COLIMA', 'HONDA COLIMA', 'ACURA COLIMA']
        })
