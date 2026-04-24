# Sistema de Comisiones Multi-Tenant

Sistema para calcular y exportar comisiones de vendedores para múltiples clientes (tenants), con estrategias de cálculo configurables por YAML.

## Arquitectura

El proyecto sigue una arquitectura DDD (Domain-Driven Design) con separación clara entre el core de negocio y los adaptadores.

```
src/
├── adapters/                         # Driving Adapters (entry points)
│   ├── cli/                          # Command Line Interface
│   │   └── main.py
│   └── api/                          # REST API (futuro)
│
└── context/                          # Bounded Contexts
    ├── commissions/                  # Contexto principal de comisiones
    │   ├── domain/                   # Lógica de negocio pura
    │   │   ├── aggregates/           # Entidades: Tenant, Plan
    │   │   ├── value_objects/        # Value Objects: OutputConfig, StrategyConfig, etc.
    │   │   ├── repositories/         # Interfaces de repositorios
    │   │   ├── services/             # Servicios de dominio
    │   │   ├── ports/                # Puertos: Exporter, StrategyFactory, ProcessingStrategy
    │   │   └── events/               # Eventos de dominio
    │   ├── application/              # Casos de uso
    │   │   ├── commands/             # CQRS Commands
    │   │   ├── queries/              # CQRS Queries
    │   │   └── dto/                  # Data Transfer Objects
    │   └── infrastructure/           # Implementaciones (Driven Adapters)
    │       ├── repositories/         # CSV, JSON, S3, BuK, API repositories
    │       ├── exporters/            # Google Sheets exporter
    │       ├── processing_strategies/# Estrategias de cálculo por cliente
    │       ├── services/             # Servicios de infraestructura
    │       └── config/               # Configuración CQRS, Strategy Factory
    │
    └── shared/                       # Código compartido entre contextos
        ├── domain/
        │   ├── value_object.py       # ValueObject base abstracto
        │   ├── cqrs/                 # Command, Query, Handler abstractions
        │   └── strategies/           # Data merge strategies
        └── infrastructure/
            ├── di/                   # Dependency Injection container
            ├── cqrs/                 # CommandBus, QueryBus implementations
            ├── file_parsers/         # CSV parser compartido
            └── validators/           # Validadores de DataFrame

config/
└── plans/                            # Configuración YAML por tenant
    ├── COPEC.yaml
    ├── GOCAR.yaml
    ├── GRUPO_K.yaml
    ├── GRUPO_VANGUARDIA.yaml
    ├── LEMONTECH.yaml
    └── SCANIA.yaml
```

## Instalación

Requiere Python `^3.12` y [Poetry](https://python-poetry.org/).

```bash
# Clonar repositorio
git clone <repo-url>
cd compensation_generator

# Instalar dependencias (incluye grupo dev: pytest, ruff, import-linter)
poetry install

# Activar el entorno virtual gestionado por Poetry
poetry shell
```

### Variables de entorno

Copiar `.env.example` a `.env` y completar con credenciales reales:

```bash
cp .env.example .env
```

Variables esperadas:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` — acceso a S3 para fuentes de datos.
- `GRUPO_VANGUARDIA_API_TOKEN` — token para la API de Grupo Vanguardia.
- `GOOGLE_APPLICATION_CREDENTIALS` — path al JSON de service account para Google Sheets (por ejemplo `config/google_credentials.json`).

## CLI - Procesamiento de Comisiones

### Comandos Disponibles

```bash
# Dentro del shell de Poetry (o con `poetry run` prefijando cada comando)
python -m src.adapters.cli.main --help
```

### Listar Tenants

```bash
python -m src.adapters.cli.main --list-tenants
```

Salida (ejemplo):
```
Available Tenants:
--------------------------------------------------------------------------------
COPEC                Copec S.A.                     [ACTIVE] 12 plans
GRUPO_K              Grupo K                        [ACTIVE] 10 plans
GRUPO_VANGUARDIA     Grupo Vanguardia               [ACTIVE] 3 plans
SCANIA               Scania Chile S.A.              [ACTIVE] 32 plans
GOCAR                Gocar Auto                     [ACTIVE] 2 plans
LEMONTECH            Lemontech                      [ACTIVE] 4 plans
```

### Listar Planes de un Tenant

```bash
python -m src.adapters.cli.main --tenant COPEC --list-plans
```

Salida (ejemplo):
```
Plans for Tenant: Copec S.A. (COPEC)
--------------------------------------------------------------------------------
[OK] PLAN_800        PLAN TCT - Clientes Nuevos        [ACTIVE]
[OK] PLAN_806        PLAN TAE - Clientes Nuevos        [ACTIVE]
[OK] PLAN_835        PLAN CE - Clientes Nuevos         [ACTIVE]
[OK] PLAN_836        PLAN AppCE - Clientes Nuevos      [ACTIVE]
[OK] PLAN_837        PLAN TCT Premium - Patentes Nuevas [ACTIVE]
[OK] PLAN_842        PLAN Lubricantes                  [ACTIVE]
[OK] PLAN_786        Bono Cumplimiento POA Volumen     [ACTIVE]
[OK] PLAN_856        Bono Cumplimiento POA Margen      [ACTIVE]
...
```

### Procesar Comisiones

```bash
# Procesar todos los planes de un tenant
python -m src.adapters.cli.main --tenant SCANIA

# Procesar planes específicos
python -m src.adapters.cli.main --tenant COPEC --plans PLAN_800 PLAN_806

# Procesar todos los tenants activos
python -m src.adapters.cli.main --all

# Con modo debug (logs detallados)
python -m src.adapters.cli.main --tenant SCANIA --debug
```

### Opciones Adicionales

| Opción | Descripción |
|--------|-------------|
| `--tenant ID` | ID del tenant a procesar |
| `--plans P1 P2` | IDs de planes específicos (opcional) |
| `--list-tenants` | Lista todos los tenants disponibles |
| `--list-plans` | Lista planes del tenant (requiere --tenant) |
| `--all` | Procesa todos los tenants activos |
| `--plans-dir PATH` | Directorio de configuración (default: config/plans) |
| `--credentials PATH` | Ruta a credenciales de Google para GSheets |
| `--debug` | Activa logging detallado |

## Tests y Linting

```bash
# Ejecutar todos los tests unitarios (la cobertura se gatilla por pyproject.toml)
pytest tests/unit/

# Ejecutar tests de un módulo específico
pytest tests/unit/context/commissions/infrastructure/processing_strategies/copec/lubricants/test_lubricants_merge.py -v

# Linter
ruff check src/

# Verificación de arquitectura (reglas en .importlinter)
lint-imports
```

## Configuración de Tenants (YAML)

Cada tenant se configura en `config/plans/{TENANT_ID}.yaml`:

```yaml
tenant:
  id: SCANIA
  name: "SCANIA S.A."
  active: true
  gsheet_id: "1abc123..."

plans:
  PLAN_798:
    active: true
    name: "Jefes de Ventas CWS"

    data_sources:
      - id: ventas
        type: s3
        config:
          bucket: "bucket-name"
          key: "path/to/file.csv"

      - id: empleados
        type: csv
        config:
          path: "data/empleados.csv"

    data_merge_strategy:
      type: custom
      config:
        strategy_name: scania_jefe_cws_merge

    script:
      module: src.context.commissions.infrastructure.processing_strategies.custom.scania.plan_798
      class: ScaniaJefeCWSStrategy
      params:
        target_period: "2025-10-01"

    output:
      sheet_id: "1xyz789..."
      tab_name: "PLAN_798"
      clear_before_write: true
```

## Docker (Opcional)

```bash
# Levantar servicios
docker-compose up -d

# Ver logs
docker-compose logs -f

# Ver servicios
docker-compose ps

# Ejecutar la CLI dentro del contenedor
docker-compose exec web python -m src.adapters.cli.main --list-tenants

# Detener servicios
docker-compose down
```

## Flujo de Procesamiento

1. **Bootstrap**: Se inicializa el contenedor DI con todos los servicios
2. **Query**: Se obtiene el tenant y sus planes desde el YAML
3. **Data Extraction**: Se extraen datos de múltiples fuentes (S3, CSV, JSON, BuK, API)
4. **Data Merge**: Se combinan los datos según la estrategia configurada
5. **Commission Calculation**: Se aplica la estrategia de cálculo del plan
6. **Export**: Se exportan los resultados a Google Sheets

## Estructura de Eventos

El sistema emite eventos de dominio durante el procesamiento:

- `CommissionCalculated` - Cuando se calculan las comisiones de un plan
- `CommissionExported` - Cuando se exportan los resultados
- `PlanProcessingFailed` - Cuando falla el procesamiento de un plan