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

Run `uv run pytest` before opening a PR. Tests live in `src/modules/data_processing/application/test/` and follow the `test_*.py` naming convention.

## Architecture

This is an **async FastAPI service** that processes customer data files (CSV/XLSX) for communication campaigns (SMS, Email, Call Blasting). It uses **Clean Architecture** layered as `domain/` → `application/` → `infrastructure/` within each module.

### Request Flow

```
POST /v2/processing/{service}
    ↓
ProcessDataUseCase (src/modules/process/app/use_case/process.py)
    ├── FileReaderFactory → CSV or XLSX reader
    ├── LevelValidator   → enforce record limits (Level 1: max 10 rows)
    └── ProcessorFactory → service-specific processor
            ↓
        SmsProcessor (pipeline chain):
            CleanData → Exclution → AssignOperator → ConcatPrefix
```

### Key Modules

- **`src/modules/process/`** — primary module; handles SMS, Email, Call Blasting
- **`src/modules/data_processing/`** — alternate module; currently disabled in `main.py`
- **`src/modules/_common/`** — shared infrastructure: file readers (CSV/XLSX), Redis cache, DB session factories

### Database & Caching

Three async MySQL engines are created in `src/lifespan.py` at startup (`saem3`, `portabilidad`, `masivos_sms`). Redis is used to cache operator-number ranges with a 2-hour TTL (`numeration:v1:{country_id}`). Redis failures are best-effort and do not break the pipeline.

### Data Processing

- File reading and transformation use **Polars** (not Pandas).
- Phone number normalization lives in `src/modules/process/app/normalizers/`.
- Operator assignment (`NumerationService`) uses cached range lookups from the `portabilidad` database.
- Processing steps are **pipeline objects** implementing `IPipeline`; each mutates a shared Polars DataFrame.

## Layer Rules

- **`domain/`** — entities, enums, interfaces/protocols, constants. No framework imports.
- **`application/`** — use cases, services, pipelines, factories. Depends on domain only.
- **`infrastructure/`** — SQLAlchemy models/repos, FastAPI routes, Redis client, file I/O.

## Configuration

Copy `.env.template` to `.env`. Key variables:

| Variable | Purpose |
|---|---|
| `DB_SAEM3` / `DB_PORTABILIDAD` / `DB_MASIVOS_SMS` | MySQL DSNs |
| `REDIS_URL` | Redis connection (default `localhost:6379`) |
| `REPOSITORY_FILES_DIR` | Base path for uploaded data files |
| `PREFIX_APP` | API prefix (default `/v2`) |
| `ENV` | `dev` or `prod` |

New environment variables must be added to both `.env.template` and `src/config/settings.py`.

## Conventions

- Snake_case for files/functions/variables; PascalCase for classes; UPPERCASE for settings/constants.
- Commit messages follow Conventional Commits with scopes: `feat(models):`, `refactor(sms):`, etc.
- Runtime outputs go to `resultados/`.
