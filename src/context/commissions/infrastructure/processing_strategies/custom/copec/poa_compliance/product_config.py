from dataclasses import dataclass


@dataclass(frozen=True)
class ProductConfig:
    name: str
    source_key: str
    product_filter: str | list[str] | None
    volume_column: str
    contribution_column: str | None = None
    volume_divisor: float = 1.0
    volume_unit: str = "M3"
    margin_unit: str = "$/M3"


PRODUCT_CONFIGS = [
    ProductConfig('TCT', 'TCT_TAE', 'TCT', 'volumen', 'contribucion', 1000.0, 'M3', '$/M3'),
    ProductConfig('TAE', 'TCT_TAE', 'TAE', 'volumen', 'contribucion', 1000.0, 'M3', '$/M3'),
    ProductConfig('CE', 'CUPON_ELECTRONICO', 'Cupon Electronico', 'volumen', 'contribucion', 1000.0, 'M3', None),
    ProductConfig('AppCE', 'APP_COPEC', 'App Copec Empresa Combustible', 'volumen', None, 1000.0, 'M3', None),
    ProductConfig(
        'Bluemax TCT', 'BLUEMAX', ['Bluemax Indirecto TCT', 'Bluemax Directo TCT'],
        'volumen_lts', 'contribucion', 1000.0, 'M3', '$/M3'
    ),
    ProductConfig('Bluemax AppCE', 'APP_COPEC', 'App Copec Empresa Bluemax', 'volumen', None, 1000.0, 'M3', None),
    ProductConfig('Lubricantes', 'LUBRICANTES', None, 'volumen', None, 1.0, 'L', None),
    ProductConfig('TCTP', 'TCT_PREMIUM', 'TCT Premium', 'volumen_tct_premium', None, 1.0, 'N patentes', None),
]
