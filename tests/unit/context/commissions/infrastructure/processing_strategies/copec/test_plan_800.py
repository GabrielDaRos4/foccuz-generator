import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.copec import (
    copec_new_client_merge,
)
from src.context.commissions.infrastructure.processing_strategies.custom.copec.new_client import (
    CopecNewClientCommissionStrategy,
)


class TestCopecNewClientCommissionStrategy:

    def test_identifica_clientes_nuevos_correctamente(self):
        ventas_actual = pd.DataFrame({
            'producto': ['TCT', 'TCT', 'TCT'],
            'ejecutivo': ['0001000206', '0001000206', '0001000206'],
            'rut_ejecutivo': ['12345678', '12345678', '12345678'],
            'dv_ejecutivo': ['9', '9', '9'],
            'rut_cliente': ['11111111', '22222222', '33333333'],
            'dv_cliente': ['1', '2', '3'],
            'volumen': [10.0, 20.0, 15.0],
            'descuento': [50.0, 100.0, 80.0],
            'anio': ['2025', '2025', '2025'],
            'mes': ['10', '10', '10']
        })

        ventas_m1 = pd.DataFrame({
            'producto': ['TCT', 'TCT'],
            'rut_cliente': ['11111111', '44444444'],
            'dv_cliente': ['1', '4'],
            'volumen': [5.0, 10.0],
            'descuento': [25.0, 50.0]
        })

        ventas_m2 = pd.DataFrame({
            'producto': ['TCT'],
            'rut_cliente': ['11111111'],
            'dv_cliente': ['1'],
            'volumen': [8.0],
            'descuento': [40.0]
        })

        ventas_m3 = pd.DataFrame({
            'producto': ['TCT'],
            'rut_cliente': ['11111111'],
            'dv_cliente': ['1'],
            'volumen': [8.0],
            'descuento': [40.0]
        })

        ventas_actual.attrs['ventas_historicas'] = [ventas_m1, ventas_m2, ventas_m3]
        ventas_actual.attrs['empleados'] = None

        strategy = CopecNewClientCommissionStrategy(
            product_type='TCT',
            discount_percentage=0.08,
            max_factor=6.0,
            rep_id_filter='0001000206',
            factor_minimo=0.5,
            bono_nuevo=10000
        )

        result = strategy.calculate_commission(ventas_actual)

        assert len(result) == 2
        ruts_nuevos = result['Rut Cliente'].tolist()
        assert '22222222-2' in ruts_nuevos
        assert '33333333-3' in ruts_nuevos
        assert '11111111-1' not in ruts_nuevos

    def test_calcula_comision_correctamente_por_cliente(self):
        ventas_actual = pd.DataFrame({
            'producto': ['TCT'],
            'ejecutivo': ['0001000206'],
            'rut_ejecutivo': ['12345678'],
            'dv_ejecutivo': ['9'],
            'rut_cliente': ['99999999'],
            'dv_cliente': ['9'],
            'volumen': [1000.0],
            'descuento': [100.0],
            'anio': ['2025'],
            'mes': ['10']
        })

        ventas_actual.attrs['ventas_historicas'] = []
        ventas_actual.attrs['empleados'] = None

        strategy = CopecNewClientCommissionStrategy(
            product_type='TCT',
            discount_percentage=0.08,
            max_factor=6.0,
            rep_id_filter='0001000206',
            factor_minimo=0.5,
            bono_nuevo=10000
        )

        result = strategy.calculate_commission(ventas_actual)

        assert len(result) == 1

        total_discount = 100.0
        volume = 1000.0
        unit_discount = total_discount / volume
        unit_commission = 6.0 - (unit_discount * 0.08)
        commission_amount = max(unit_commission * volume, 0.5)
        new_client_bonus = 10000
        total_commission = commission_amount + new_client_bonus

        assert result['Comision'].iloc[0] == pytest.approx(total_commission, rel=0.01)
        assert result['Bono Cliente Nuevo'].iloc[0] == 10000

    def test_aplica_margen_minimo_cuando_es_mayor(self):
        ventas_actual = pd.DataFrame({
            'producto': ['TCT'],
            'ejecutivo': ['0001000206'],
            'rut_ejecutivo': ['12345678'],
            'dv_ejecutivo': ['9'],
            'rut_cliente': ['88888888'],
            'dv_cliente': ['8'],
            'volumen': [100.0],
            'descuento': [500.0],
            'anio': ['2025'],
            'mes': ['10']
        })

        ventas_actual.attrs['ventas_historicas'] = []
        ventas_actual.attrs['empleados'] = None

        strategy = CopecNewClientCommissionStrategy(
            product_type='TCT',
            discount_percentage=0.08,
            max_factor=6.0,
            rep_id_filter='0001000206',
            factor_minimo=1000.0,
            bono_nuevo=10000
        )

        result = strategy.calculate_commission(ventas_actual)

        factor_minimo = 1000.0
        volume = 100.0
        bono_nuevo = 10000
        expected_unit_commission = factor_minimo
        expected_total_commission = (factor_minimo * volume) + bono_nuevo

        assert result['Comision $/L'].iloc[0] == expected_unit_commission
        assert result['Comision'].iloc[0] == expected_total_commission

    def test_retorna_vacio_si_no_hay_clientes_nuevos(self):
        ventas_actual = pd.DataFrame({
            'producto': ['TCT'],
            'ejecutivo': ['0001000206'],
            'rut_ejecutivo': ['12345678'],
            'dv_ejecutivo': ['9'],
            'rut_cliente': ['11111111'],
            'dv_cliente': ['1'],
            'volumen': [10.0],
            'descuento': [50.0],
            'anio': ['2025'],
            'mes': ['10']
        })

        ventas_m1 = pd.DataFrame({
            'producto': ['TCT'],
            'rut_cliente': ['11111111'],
            'dv_cliente': ['1'],
            'volumen': [5.0],
            'descuento': [25.0]
        })

        ventas_m2 = pd.DataFrame({
            'producto': ['TCT'],
            'rut_cliente': ['11111111'],
            'dv_cliente': ['1'],
            'volumen': [5.0],
            'descuento': [25.0]
        })

        ventas_m3 = pd.DataFrame({
            'producto': ['TCT'],
            'rut_cliente': ['11111111'],
            'dv_cliente': ['1'],
            'volumen': [5.0],
            'descuento': [25.0]
        })

        ventas_actual.attrs['ventas_historicas'] = [ventas_m1, ventas_m2, ventas_m3]
        ventas_actual.attrs['empleados'] = None

        strategy = CopecNewClientCommissionStrategy(
            product_type='TCT',
            discount_percentage=0.08,
            max_factor=6.0,
            rep_id_filter='0001000206',
            factor_minimo=0.5,
            bono_nuevo=10000
        )

        result = strategy.calculate_commission(ventas_actual)

        assert len(result) == 0

    def test_different_product_type_tae(self):
        ventas_actual = pd.DataFrame({
            'producto': ['TAE'],
            'ejecutivo': ['0001000206'],
            'rut_ejecutivo': ['12345678'],
            'dv_ejecutivo': ['9'],
            'rut_cliente': ['99999999'],
            'dv_cliente': ['9'],
            'volumen': [100.0],
            'descuento': [50.0],
            'anio': ['2025'],
            'mes': ['10']
        })

        ventas_actual.attrs['ventas_historicas'] = []
        ventas_actual.attrs['empleados'] = None

        strategy = CopecNewClientCommissionStrategy(
            product_type='TAE',
            discount_percentage=0.08,
            max_factor=6.0,
            bono_nuevo=10000
        )

        result = strategy.calculate_commission(ventas_actual)

        assert len(result) == 1

    def test_different_product_type_cupon_electronico(self):
        ventas_actual = pd.DataFrame({
            'producto': ['CUPON ELECTRONICO'],
            'ejecutivo': ['0001000206'],
            'rut_ejecutivo': ['12345678'],
            'dv_ejecutivo': ['9'],
            'rut_cliente': ['99999999'],
            'dv_cliente': ['9'],
            'volumen': [100.0],
            'descuento': [50.0],
            'anio': ['2025'],
            'mes': ['10']
        })

        ventas_actual.attrs['ventas_historicas'] = []
        ventas_actual.attrs['empleados'] = None

        strategy = CopecNewClientCommissionStrategy(
            product_type='CUPON ELECTRONICO',
            discount_percentage=0.25,
            max_factor=10.0,
            bono_nuevo=0
        )

        result = strategy.calculate_commission(ventas_actual)

        assert len(result) == 1
        assert 'Bono Cliente Nuevo' not in result.columns


class TestCopecNewClientMerge:

    def test_merge_combina_datos_historicos(self):
        ventas_actual = pd.DataFrame({'col1': [1, 2]})
        ventas_m1 = pd.DataFrame({'col1': [3, 4]})
        ventas_m2 = pd.DataFrame({'col1': [5, 6]})
        ventas_m3 = pd.DataFrame({'col1': [7, 8]})
        empleados = pd.DataFrame({'col2': ['A', 'B']})

        dataframes = {
            'ventas_mes_actual': ventas_actual,
            'ventas_mes_1': ventas_m1,
            'ventas_mes_2': ventas_m2,
            'ventas_mes_3': ventas_m3,
            'empleados': empleados
        }

        result = copec_new_client_merge(dataframes)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'ventas_historicas' in result.attrs
        assert 'empleados' in result.attrs
        assert len(result.attrs['ventas_historicas']) == 3

    def test_merge_maneja_datos_historicos_faltantes(self):
        ventas_actual = pd.DataFrame({'col1': [1, 2]})

        dataframes = {
            'ventas_mes_actual': ventas_actual
        }

        result = copec_new_client_merge(dataframes)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'ventas_historicas' in result.attrs
        assert len(result.attrs['ventas_historicas']) == 0

    def test_elimina_duplicados_por_cliente(self):
        ventas_actual = pd.DataFrame({
            'producto': ['TCT', 'TCT'],
            'ejecutivo': ['0001000206', '0001000206'],
            'rut_ejecutivo': ['12345678', '12345678'],
            'dv_ejecutivo': ['9', '9'],
            'rut_cliente': ['11111111', '11111111'],
            'dv_cliente': ['1', '1'],
            'volumen': [10.0, 15.0],
            'descuento': [50.0, 50.0],
            'anio': ['2025', '2025'],
            'mes': ['10', '10']
        })

        ventas_actual.attrs['ventas_historicas'] = []
        ventas_actual.attrs['empleados'] = None

        strategy = CopecNewClientCommissionStrategy(
            product_type='TCT',
            discount_percentage=0.08,
            max_factor=6.0,
            rep_id_filter='0001000206',
            factor_minimo=0.5,
            bono_nuevo=10000
        )

        result = strategy.calculate_commission(ventas_actual)

        assert len(result) == 1

    def test_calcula_periodo_desde_datos(self):
        ventas_actual = pd.DataFrame({
            'producto': ['TCT'],
            'ejecutivo': ['0001000206'],
            'rut_ejecutivo': ['12345678'],
            'dv_ejecutivo': ['9'],
            'rut_cliente': ['99999999'],
            'dv_cliente': ['9'],
            'volumen': [100.0],
            'descuento': [50.0],
            'anio': ['2025'],
            'mes': ['10']
        })

        ventas_actual.attrs['ventas_historicas'] = []
        ventas_actual.attrs['empleados'] = None

        strategy = CopecNewClientCommissionStrategy(
            product_type='TCT',
            discount_percentage=0.08,
            max_factor=6.0,
            rep_id_filter='0001000206',
            factor_minimo=0.5,
            bono_nuevo=10000
        )

        result = strategy.calculate_commission(ventas_actual)

        assert len(result) == 1
        assert 'Fecha' in result.columns
        assert result['Fecha'].iloc[0] == '2025-10-01'
