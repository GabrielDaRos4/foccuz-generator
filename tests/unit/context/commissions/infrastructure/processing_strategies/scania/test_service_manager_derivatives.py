import pandas as pd

from src.context.commissions.infrastructure.processing_strategies.custom.scania import (
    BaseScaniaStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.service_manager import (
    AdminManagerStrategy,
    AsesorServicioStrategy,
    DesaboManagerStrategy,
    EncargadoAdmStrategy,
    EncargadoRBStrategy,
    JefeDesaboStrategy,
    JefeTallerStrategy,
    RBManagerStrategy,
    ServiceAdvisorStrategy,
    SupervisorTallerStrategy,
    WorkshopManagerStrategy,
    WorkshopSupervisorStrategy,
)


class TestWorkshopManagerStrategy:

    def test_should_inherit_from_service_manager(self):
        assert issubclass(WorkshopManagerStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert JefeTallerStrategy is WorkshopManagerStrategy

    def test_should_calculate_commission(self):
        strategy = WorkshopManagerStrategy(role_filter=["jefe de taller"])
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe de taller',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.10,
            'resultado_nps': 0.90,
            'tamano': 'mediana'
        }])

        result = strategy.calculate_commission(data)

        assert 'Comisión' in result.columns
        assert result['Comisión'].iloc[0] > 0


class TestWorkshopSupervisorStrategy:

    def test_should_inherit_from_service_manager(self):
        assert issubclass(WorkshopSupervisorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert SupervisorTallerStrategy is WorkshopSupervisorStrategy

    def test_should_calculate_commission(self):
        strategy = WorkshopSupervisorStrategy(role_filter=["supervisor de taller"])
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'supervisor de taller',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.10,
            'resultado_nps': 0.90,
            'tamano': 'mediana'
        }])

        result = strategy.calculate_commission(data)

        assert 'Comisión' in result.columns


class TestServiceAdvisorStrategy:

    def test_should_inherit_from_service_manager(self):
        assert issubclass(ServiceAdvisorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert AsesorServicioStrategy is ServiceAdvisorStrategy

    def test_should_calculate_commission(self):
        strategy = ServiceAdvisorStrategy(role_filter=["asesor de servicios"])
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'asesor de servicios',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.10,
            'resultado_nps': 0.90,
            'tamano': 'mediana'
        }])

        result = strategy.calculate_commission(data)

        assert 'Comisión' in result.columns


class TestRBManagerStrategy:

    def test_should_inherit_from_service_manager(self):
        assert issubclass(RBManagerStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert EncargadoRBStrategy is RBManagerStrategy

    def test_should_calculate_commission(self):
        strategy = RBManagerStrategy(role_filter=["vendedor de repuestos"])
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'vendedor de repuestos',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.10,
            'resultado_nps': 0.90,
            'tamano': 'mediana'
        }])

        result = strategy.calculate_commission(data)

        assert 'Comisión' in result.columns


class TestAdminManagerStrategy:

    def test_should_inherit_from_service_manager(self):
        assert issubclass(AdminManagerStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert EncargadoAdmStrategy is AdminManagerStrategy

    def test_should_calculate_commission(self):
        strategy = AdminManagerStrategy(role_filter=["encargado de administracion"])
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'encargado de administracion',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.10,
            'resultado_nps': 0.90,
            'tamano': 'mediana'
        }])

        result = strategy.calculate_commission(data)

        assert 'Comisión' in result.columns


class TestDesaboManagerStrategy:

    def test_should_inherit_from_service_manager(self):
        assert issubclass(DesaboManagerStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert JefeDesaboStrategy is DesaboManagerStrategy

    def test_should_calculate_commission(self):
        strategy = DesaboManagerStrategy(role_filter=["jefe de desabolladuria"])
        data = pd.DataFrame([{
            'id empleado': 1,
            'rut': '16.766.611-6',
            'cargo': 'jefe de desabolladuria',
            'branchid': 'SCL001',
            'days_worked': 30,
            'cumplimiento venta': 1.10,
            'resultado_nps': 0.90,
            'tamano': 'mediana'
        }])

        result = strategy.calculate_commission(data)

        assert 'Comisión' in result.columns
