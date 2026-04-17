# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
uv sync                          # Install dependencies
uv run fastapi dev src/main.py   # Start API with hot reload
uv run pytest                    # Run test suite
uv run python -m src.main        # Import validation only
```

If `uv` is unavailable: `pip install -r requirements.txt`.

Run `uv run pytest` before opening a PR. Tests live in `src/modules/process/test/` and follow the `test_*.py` naming convention. Run a single test with:

```bash
uv run pytest src/modules/process/test/test_sms_flow.py -v
uv run pytest src/modules/process/test/test_sms_flow.py::test_sms_full_pipeline -v
```

## Architecture

This is an **async FastAPI service** that processes customer data files (CSV/XLSX) for communication campaigns (SMS, Email, Call Blasting). It uses **Clean Architecture** layered as `domain/` → `application/` → `infrastructure/` within each module.

### Request Flow

```
POST /v2/processing/{service}
    ↓ DataProcessingDTO (JSON payload)
ProcessDataUseCase (src/modules/process/app/use_case/process.py)
    ├── ReaderFileFactory   → CSV or XLSX Polars DataFrame
    ├── LevelValidator      → Level 1: max 10 rows | Level >1: max 700,000 rows
    └── ProcessorFactory    → service-specific processor
            ↓
        SmsProcessor (pipeline chain, ~11 steps):
            CleanData → Exclution → AssignOperator → ConcatPrefix
            → AssignCost → CustomMessage → Landing → CalculatePDU
            → ValidateRegulations → CalculateCredits → SaveResults
            ↓
        Returns JSON: { summaryGeneral, summaryGroup, violations }

POST /v2/confirm/{service}    # confirms processed data into upstream DB
```

### Key Modules

- **`src/modules/process/`** — primary module; SMS, Email, Call Blasting (active)
- **`src/modules/data_processing/`** — legacy module; disabled in `main.py`
- **`src/modules/_common/`** — shared infrastructure: file readers, Redis cache, DB session factories

### Pipeline Pattern

All processing steps implement `IPipeline` (`src/modules/process/domain/interfaces/pipeline.py`):

```python
async def execute(self, df: pl.DataFrame | pl.LazyFrame, ctx: DataProcessingDTO) -> pl.DataFrame
```

Each step receives the shared `DataProcessingDTO` for campaign config and returns a transformed DataFrame. Internal/computed columns use a `__COLUMN__` naming convention (e.g., `__IS_OK__`, `__CREDITS__`, `__OPERATOR__`). All column names are constants defined in `src/modules/process/domain/constants/cols.py` as `Cols`.

### Service Processors

| Service | Key difference |
|---|---|
| **SMS** | Calculates PDU (160 chars = 1 PDU; special chars = 70), validates regulations (shortname, char limits), supports `landing` sub-service |
| **Email** | Uses Polars `LazyFrame` for deferred evaluation; no operator assignment; groups summary by `email_domain` |
| **Call Blasting** | Two sub-services: `standard` (fixed audio duration) and `custom` (custom TTS message); cost includes initial + incremental seconds |

### Database & Caching

Five async MySQL engines are created in `src/lifespan.py` at startup:

| Engine key | Database variable | Purpose |
|---|---|---|
| `saem3` | `DB_SAEM3` | Tariff costs (`TelCost`) |
| `portabilidad` | `DB_PORTABILIDAD` | Operator number ranges (`Numeracion`) |
| `masivos_sms` | `DB_MASIVOS_SMS` | (reserved) |
| `telefonos_campanas` | `DB_TELEFONOS_CAMPANAS` | SMS confirm lookups |
| `email` | `DB_EMAIL` | Email confirm lookups |

Redis caches operator ranges (`numeration:v1:{country_id}`, TTL 2h) and tariff costs (`cost:t{tariff_id}:c{country_id}:s{service}`, TTL 5m). Redis failures are best-effort and do not break the pipeline.

Shared deps (repositories, cache client) are built once at startup in `lifespan.py` and stored on `app.state`. DB sessions are injected per-request via FastAPI `Depends()`.

### Data Processing

- File reading and transformation use **Polars** (not Pandas).
- Phone number normalization: `src/modules/process/app/normalizers/number.py`.
- Operator assignment (`NumerationService`) uses NumPy binary search over cached ranges from the `portabilidad` DB.
- Exclusion reasons are typed constants in `src/modules/process/domain/constants/reasons.py` (`ExclusionReason`).
- Results are saved as Parquet files under `resultados/Campaign/{service}/`.

## Layer Rules

- **`domain/`** — entities, enums, interfaces/protocols, constants. No framework imports.
- **`application/`** — use cases, services, pipelines, factories. Depends on domain only.
- **`infrastructure/`** — SQLAlchemy models/repos, FastAPI routes, Redis client, file I/O.

## Configuration

Copy `.env.template` to `.env`. Key variables:

| Variable | Purpose |
|---|---|
| `DB_SAEM3` | MySQL DSN — tariff costs |
| `DB_PORTABILIDAD` | MySQL DSN — operator ranges |
| `DB_MASIVOS_SMS` | MySQL DSN — SMS masivos |
| `DB_TELEFONOS_CAMPANAS` | MySQL DSN — SMS confirm |
| `DB_EMAIL` | MySQL DSN — email confirm |
| `REDIS_URL` | Redis connection (default `redis://localhost:6379/0`) |
| `REPOSITORY_FILES_DIR` | Base path for uploaded data files |
| `OUTPUT_DIR` | Output directory for Parquet files (default `resultados`) |
| `PREFIX_APP` | API prefix (default `/v2`) |
| `ENV` | `dev` or `prod` |

New environment variables must be added to both `.env.template` and `src/config/settings.py`.

## Conventions

- Snake_case for files/functions/variables; PascalCase for classes; UPPERCASE for settings/constants.
- Commit messages follow Conventional Commits with scopes: `feat(models):`, `refactor(sms):`, etc.
- Internal DataFrame columns are named `__COLUMN__` (double underscores) and defined as constants in `Cols`.
- Runtime outputs (Parquet files) go to `resultados/`.
