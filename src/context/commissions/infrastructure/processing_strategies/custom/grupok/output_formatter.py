import pandas as pd


class GrupoKOutputFormatter:

    COLUMN_TYPES = {
        'Fecha': 'date',
        'Rep ID': 'text',
        'ID Transaccion': 'text',
        'Vendedor': 'text',
        'Sucursal': 'text',
        'Cantidad Ventas': 'number',
        'Monto Neto': 'money',
        'Comision': 'money',
    }

    COLUMN_ORDER = [
        'Fecha',
        'Rep ID',
        'ID Transaccion',
        'Vendedor',
        'Sucursal',
        'Cantidad Ventas',
        'Monto Neto',
        'Comision',
    ]

    STORE_MANAGER_COLUMN_TYPES = {
        'Fecha': 'date',
        'Rep ID': 'text',
        'ID Transaccion': 'text',
        'Jefe de Tienda': 'text',
        'RUT': 'text',
        'Sucursal': 'text',
        'Cantidad Ventas': 'number',
        'Total Ventas': 'money',
        'Comision': 'money',
    }

    STORE_MANAGER_COLUMN_ORDER = [
        'Fecha',
        'Rep ID',
        'ID Transaccion',
        'Jefe de Tienda',
        'RUT',
        'Sucursal',
        'Cantidad Ventas',
        'Total Ventas',
        'Comision',
    ]

    PM_COLUMN_TYPES = {
        'Fecha': 'date',
        'Rep ID': 'text',
        'ID Transaccion': 'text',
        'Product Manager': 'text',
        'RUT': 'text',
        'Linea Negocio': 'text',
        'Cantidad Ventas': 'number',
        'Total Ventas': 'money',
        'Comision': 'money',
    }

    PM_COLUMN_ORDER = [
        'Fecha',
        'Rep ID',
        'ID Transaccion',
        'Product Manager',
        'RUT',
        'Linea Negocio',
        'Cantidad Ventas',
        'Total Ventas',
        'Comision',
    ]

    SUBGERENTE_COLUMN_TYPES = {
        'Fecha': 'date',
        'Rep ID': 'text',
        'ID Transaccion': 'text',
        'Subgerente': 'text',
        'RUT': 'text',
        'Product Manager': 'text',
        'Linea Negocio': 'text',
        'Cantidad Ventas': 'number',
        'Total Ventas': 'money',
        'Comision': 'money',
    }

    SUBGERENTE_COLUMN_ORDER = [
        'Fecha',
        'Rep ID',
        'ID Transaccion',
        'Subgerente',
        'RUT',
        'Product Manager',
        'Linea Negocio',
        'Cantidad Ventas',
        'Total Ventas',
        'Comision',
    ]

    def format(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = self._reorder_columns(df)
        df = self._clean_data(df)
        df = self._sort_data(df)
        df.attrs['column_types'] = self.COLUMN_TYPES

        return df

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = [col for col in self.COLUMN_ORDER if col in df.columns]
        extra_columns = [col for col in df.columns if col not in self.COLUMN_ORDER]
        return df[columns + extra_columns]

    @staticmethod
    def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if 'Rep ID' in df.columns:
            df = df[df['Rep ID'].notna() & (df['Rep ID'] != '')].copy()

        if 'Monto Neto' in df.columns:
            df['Monto Neto'] = pd.to_numeric(df['Monto Neto'], errors='coerce').fillna(0).round(0).astype(int)

        if 'Comision' in df.columns:
            df['Comision'] = pd.to_numeric(df['Comision'], errors='coerce').fillna(0).round(0).astype(int)

        if 'Cantidad Ventas' in df.columns:
            df['Cantidad Ventas'] = pd.to_numeric(df['Cantidad Ventas'], errors='coerce').fillna(0).astype(int)

        return df

    @staticmethod
    def _sort_data(df: pd.DataFrame) -> pd.DataFrame:
        sort_columns = []
        if 'Sucursal' in df.columns:
            sort_columns.append('Sucursal')
        if 'Rep ID' in df.columns:
            sort_columns.append('Rep ID')

        if sort_columns:
            return df.sort_values(sort_columns).reset_index(drop=True)

        return df

    def format_store_manager(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = self._reorder_store_manager_columns(df)
        df = self._clean_store_manager_data(df)
        df = self._sort_store_manager_data(df)
        df.attrs['column_types'] = self.STORE_MANAGER_COLUMN_TYPES

        return df

    def _reorder_store_manager_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = [col for col in self.STORE_MANAGER_COLUMN_ORDER if col in df.columns]
        extra_columns = [col for col in df.columns if col not in self.STORE_MANAGER_COLUMN_ORDER]
        return df[columns + extra_columns]

    @staticmethod
    def _clean_store_manager_data(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if 'Rep ID' in df.columns:
            df = df[df['Rep ID'].notna() & (df['Rep ID'] != '')].copy()

        if 'Total Ventas' in df.columns:
            df['Total Ventas'] = pd.to_numeric(df['Total Ventas'], errors='coerce').fillna(0).round(0).astype(int)

        if 'Comision' in df.columns:
            df['Comision'] = pd.to_numeric(df['Comision'], errors='coerce').fillna(0).round(0).astype(int)

        if 'Cantidad Ventas' in df.columns:
            df['Cantidad Ventas'] = pd.to_numeric(df['Cantidad Ventas'], errors='coerce').fillna(0).astype(int)

        return df

    @staticmethod
    def _sort_store_manager_data(df: pd.DataFrame) -> pd.DataFrame:
        sort_columns = []
        if 'Sucursal' in df.columns:
            sort_columns.append('Sucursal')
        if 'Rep ID' in df.columns:
            sort_columns.append('Rep ID')

        if sort_columns:
            return df.sort_values(sort_columns).reset_index(drop=True)

        return df

    def format_product_manager(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = self._reorder_pm_columns(df)
        df = self._clean_pm_data(df)
        df.attrs['column_types'] = self.PM_COLUMN_TYPES

        return df

    def _reorder_pm_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = [col for col in self.PM_COLUMN_ORDER if col in df.columns]
        extra_columns = [col for col in df.columns if col not in self.PM_COLUMN_ORDER]
        return df[columns + extra_columns]

    @staticmethod
    def _clean_pm_data(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if 'Rep ID' in df.columns:
            df = df[df['Rep ID'].notna() & (df['Rep ID'] != '')].copy()

        if 'Total Ventas' in df.columns:
            df['Total Ventas'] = pd.to_numeric(df['Total Ventas'], errors='coerce').fillna(0).round(0).astype(int)

        if 'Comision' in df.columns:
            df['Comision'] = pd.to_numeric(df['Comision'], errors='coerce').fillna(0).round(0).astype(int)

        if 'Cantidad Ventas' in df.columns:
            df['Cantidad Ventas'] = pd.to_numeric(df['Cantidad Ventas'], errors='coerce').fillna(0).astype(int)

        return df

    def format_subgerente(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = self._reorder_subgerente_columns(df)
        df = self._clean_subgerente_data(df)
        df.attrs['column_types'] = self.SUBGERENTE_COLUMN_TYPES

        return df

    def _reorder_subgerente_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = [col for col in self.SUBGERENTE_COLUMN_ORDER if col in df.columns]
        extra_columns = [col for col in df.columns if col not in self.SUBGERENTE_COLUMN_ORDER]
        return df[columns + extra_columns]

    @staticmethod
    def _clean_subgerente_data(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        if 'Rep ID' in df.columns:
            df = df[df['Rep ID'].notna() & (df['Rep ID'] != '')].copy()

        if 'Total Ventas' in df.columns:
            df['Total Ventas'] = pd.to_numeric(df['Total Ventas'], errors='coerce').fillna(0).round(0).astype(int)

        if 'Comision' in df.columns:
            df['Comision'] = pd.to_numeric(df['Comision'], errors='coerce').fillna(0).round(0).astype(int)

        if 'Cantidad Ventas' in df.columns:
            df['Cantidad Ventas'] = pd.to_numeric(df['Cantidad Ventas'], errors='coerce').fillna(0).astype(int)

        return df
