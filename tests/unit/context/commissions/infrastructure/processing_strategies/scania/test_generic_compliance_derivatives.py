import pandas as pd
import pytest

from src.context.commissions.infrastructure.processing_strategies.custom.scania import (
    BaseScaniaStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.claims import (
    AsesorSiniestrosStrategy,
    ClaimsAdvisorStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.compliance import (
    AdministrativoCDStrategy,
    AsesorComercialServiciosStrategy,
    AsistenciaTecnicaStrategy,
    BusSalesExecutiveStrategy,
    CDAdminStrategy,
    CDOperatorStrategy,
    CommercialManagerStrategy,
    ControlTowerStrategy,
    CoordinadorMercadoStrategy,
    CoordinadorMotoresStrategy,
    EjecutivoVentaBusesStrategy,
    EjecutivoVentaNuevoStrategy,
    EjecutivoVentaUsadoStrategy,
    EngineCoordinatorStrategy,
    GenericComplianceStrategy,
    JefeComercialStrategy,
    JefeZonaStrategy,
    MarketCoordinatorStrategy,
    NewSalesExecutiveStrategy,
    OperarioCDStrategy,
    OperarioRegionesStrategy,
    OperarioSantiagoStrategy,
    PartsSalesRepStrategy,
    PresalesStrategy,
    PreventaStrategy,
    RegionsOperatorStrategy,
    SantiagoOperatorStrategy,
    ServicesCommercialAdvisorStrategy,
    TechnicalAssistanceStrategy,
    TorreControlStrategy,
    UsedSalesExecutiveStrategy,
    VendedorRepuestosStrategy,
    ZoneManagerStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.cws import (
    CWSSupervisorStrategy,
    CWSTechnicianStrategy,
    SupervisorCWSStrategy,
    TecnicoCWSStrategy,
)
from src.context.commissions.infrastructure.processing_strategies.custom.scania.technician import (
    ClaimsTechnicianStrategy,
    MechanicTechnicianStrategy,
    TecnicoMecanicoStrategy,
    TecnicoSiniestrosStrategy,
)


@pytest.fixture
def sample_data():
    return pd.DataFrame([{
        'id empleado': 1,
        'rut': '16.766.611-6',
        'cargo': 'test role',
        'branchid': 'SCL001',
        'days_worked': 30,
        'cumplimiento': 110
    }])


class TestCDOperatorStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(CDOperatorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert OperarioCDStrategy is CDOperatorStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'operario de bodega'
        sample_data['cumplimiento venta pais'] = 110
        sample_data['nivel de servicio cd'] = 95
        sample_data['ajuste inventario'] = 1.0
        strategy = CDOperatorStrategy(role_filter=["operario de bodega"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestMechanicTechnicianStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(MechanicTechnicianStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert TecnicoMecanicoStrategy is MechanicTechnicianStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'tecnico mecanico'
        strategy = MechanicTechnicianStrategy(role_filter=["tecnico mecanico"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestPartsSalesRepStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(PartsSalesRepStrategy, GenericComplianceStrategy)

    def test_should_have_spanish_alias(self):
        assert VendedorRepuestosStrategy is PartsSalesRepStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'vendedor de repuestos'
        strategy = PartsSalesRepStrategy(role_filter=["vendedor de repuestos"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestZoneManagerStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(ZoneManagerStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert JefeZonaStrategy is ZoneManagerStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'jefe de zona'
        strategy = ZoneManagerStrategy(role_filter=["jefe de zona"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestCDAdminStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(CDAdminStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert AdministrativoCDStrategy is CDAdminStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'administrativo'
        strategy = CDAdminStrategy(role_filter=["administrativo"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestEngineCoordinatorStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(EngineCoordinatorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert CoordinadorMotoresStrategy is EngineCoordinatorStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'coordinador de motores'
        strategy = EngineCoordinatorStrategy(role_filter=["coordinador de motores"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestPresalesStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(PresalesStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert PreventaStrategy is PresalesStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'preventa'
        strategy = PresalesStrategy(role_filter=["preventa"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestControlTowerStrategy:

    def test_should_inherit_from_base_scania_strategy(self):
        assert issubclass(ControlTowerStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert TorreControlStrategy is ControlTowerStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'torre de control'
        strategy = ControlTowerStrategy(role_filter=["torre de control"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestClaimsAdvisorStrategy:

    def test_should_inherit_from_base_scania_strategy(self):
        assert issubclass(ClaimsAdvisorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert AsesorSiniestrosStrategy is ClaimsAdvisorStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'asesor de siniestros'
        strategy = ClaimsAdvisorStrategy(role_filter=["asesor de siniestros"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestClaimsTechnicianStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(ClaimsTechnicianStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert TecnicoSiniestrosStrategy is ClaimsTechnicianStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'tecnico de siniestros'
        strategy = ClaimsTechnicianStrategy(role_filter=["tecnico de siniestros"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestNewSalesExecutiveStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(NewSalesExecutiveStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert EjecutivoVentaNuevoStrategy is NewSalesExecutiveStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'ejecutivo de ventas'
        strategy = NewSalesExecutiveStrategy(role_filter=["ejecutivo de ventas"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestUsedSalesExecutiveStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(UsedSalesExecutiveStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert EjecutivoVentaUsadoStrategy is UsedSalesExecutiveStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'ejecutivo de ventas'
        strategy = UsedSalesExecutiveStrategy(role_filter=["ejecutivo de ventas"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestBusSalesExecutiveStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(BusSalesExecutiveStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert EjecutivoVentaBusesStrategy is BusSalesExecutiveStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'ejecutivo de ventas'
        strategy = BusSalesExecutiveStrategy(role_filter=["ejecutivo de ventas"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestServicesCommercialAdvisorStrategy:

    def test_should_inherit_from_base_scania(self):
        assert issubclass(ServicesCommercialAdvisorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert AsesorComercialServiciosStrategy is ServicesCommercialAdvisorStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'asesor comercial de servicios'
        strategy = ServicesCommercialAdvisorStrategy(
            role_filter=["asesor comercial de servicios"]
        )
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestCWSTechnicianStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(CWSTechnicianStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert TecnicoCWSStrategy is CWSTechnicianStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'tecnico mecanico'
        strategy = CWSTechnicianStrategy(role_filter=["tecnico mecanico"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestCWSSupervisorStrategy:

    def test_should_inherit_from_base_scania_strategy(self):
        assert issubclass(CWSSupervisorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert SupervisorCWSStrategy is CWSSupervisorStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo2'] = 'sw19 - supervisor cws'
        sample_data['diferencia inventario'] = 100000
        sample_data['total inventario'] = 10000000
        sample_data['cumplimiento venta'] = 110
        strategy = CWSSupervisorStrategy()
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestCommercialManagerStrategy:

    def test_should_inherit_from_base_scania_strategy(self):
        assert issubclass(CommercialManagerStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert JefeComercialStrategy is CommercialManagerStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo2'] = 'jefe comercial'
        sample_data['cumplimiento de venta'] = 100
        sample_data['penetracion contratos zona'] = 60
        sample_data['margen de venta'] = 12
        sample_data['% efectividad venta'] = 90
        sample_data['ponderacion efectividad'] = 50
        sample_data['dio'] = 40
        sample_data['ponderacion dio'] = 50
        strategy = CommercialManagerStrategy()
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestTechnicalAssistanceStrategy:

    def test_should_inherit_from_base_scania_strategy(self):
        assert issubclass(TechnicalAssistanceStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert AsistenciaTecnicaStrategy is TechnicalAssistanceStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo2'] = 'asistencia tecnica'
        sample_data['programa_visitas'] = 3
        sample_data['camp_tecnica_seguridad'] = 85
        sample_data['scania_assitance'] = 50
        strategy = TechnicalAssistanceStrategy()
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestSantiagoOperatorStrategy:

    def test_should_inherit_from_base_scania_strategy(self):
        assert issubclass(SantiagoOperatorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert OperarioSantiagoStrategy is SantiagoOperatorStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo2'] = 'ope. bodega santiago'
        sample_data['factcumpl'] = 108
        sample_data['factnslog'] = 97
        sample_data['difinvreal'] = 0.33
        strategy = SantiagoOperatorStrategy()
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestRegionsOperatorStrategy:

    def test_should_inherit_from_base_scania_strategy(self):
        assert issubclass(RegionsOperatorStrategy, BaseScaniaStrategy)

    def test_should_have_spanish_alias(self):
        assert OperarioRegionesStrategy is RegionsOperatorStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'operario de bodega'
        strategy = RegionsOperatorStrategy(role_filter=["operario de bodega"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns


class TestMarketCoordinatorStrategy:

    def test_should_inherit_from_generic_compliance(self):
        assert issubclass(MarketCoordinatorStrategy, GenericComplianceStrategy)

    def test_should_have_spanish_alias(self):
        assert CoordinadorMercadoStrategy is MarketCoordinatorStrategy

    def test_should_calculate_commission(self, sample_data):
        sample_data['cargo'] = 'coordinador de mercado'
        strategy = MarketCoordinatorStrategy(role_filter=["coordinador de mercado"])
        result = strategy.calculate_commission(sample_data)
        assert 'Comisión' in result.columns
