# Repository Guidelines

## Project Structure & Module Organization
Core application code lives in `src/`. `src/main.py` creates the FastAPI app and wires routes through `src/lifespan.py`. Shared configuration is under `src/config/`. Business logic is organized by module in `src/modules/`, with `process/` and `data_processing/` split into `domain/`, `application/`, and `infrastructure/` layers. Shared adapters such as file readers and cache clients live in `src/modules/_common/`. Current tests are in `src/modules/data_processing/application/test/`. Runtime outputs are written to `resultados/`.

## Build, Test, and Development Commands
Use Python 3.12 and prefer `uv` for dependency management.

- `uv sync`: install project dependencies from `uv.lock`.
- `uv run pytest`: run the test suite.
- `uv run fastapi dev src/main.py`: start the API locally with reload.
- `uv run python -m src.main`: minimal module start for import validation.

If `uv` is unavailable, `pip install -r requirements.txt` is the fallback.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, type-aware code where practical, and small focused modules. Use `snake_case` for files, functions, and variables; `PascalCase` for classes; and uppercase names for settings/constants. Preserve the current layered structure when adding code: domain rules stay in `domain/`, orchestration in `application/`, and framework or storage code in `infrastructure/`. No formatter or linter is configured in this repository yet, so keep imports tidy and match surrounding style closely.

## Testing Guidelines
Tests use `pytest`. Name files `test_*.py` and prefer explicit test names such as `test_build_required_columns`. Keep tests near the module they validate, following the existing pattern under `application/test/`. Cover both header-based and index-based flows when changing file-processing logic. Run `uv run pytest` before opening a PR.

## Commit & Pull Request Guidelines
Recent history follows Conventional Commit style, often with scopes, for example `feat(models): ...` and `refactor(sms): ...`. Keep commit messages short, imperative, and scoped when useful. PRs should include a concise summary, affected modules, test results, and any required `.env` or database/Redis setup changes. Add request or payload examples when route behavior changes.

## Configuration & Environment
Settings are loaded from `.env` via `pydantic-settings`; start from `.env.template`. Do not commit real credentials for MySQL or Redis. Document any new environment variables in both `.env.template` and the PR description.
