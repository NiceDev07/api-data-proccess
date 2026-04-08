"""
Integration tests for:
  POST /v2/processing/{service}   (SMS, Email)
  POST /v2/confirm/{service}      (SMS, Email)

Each endpoint is hit with small (2 rows), medium (100 rows) and large
(1 000 rows) datasets. All external I/O (DB, Redis) is mocked.
A timing table is printed at the end of the session.

Run:
    uv run pytest src/modules/process/test/test_integration_endpoints.py -v -s
"""

from __future__ import annotations

import time
import tempfile
from pathlib import Path
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import polars as pl
import pytest
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from modules.process.infrastructure.routes.process import (
    router as process_router,
    get_use_case,
    get_confirm_factory,
)
from modules.process.app.use_case.process import ProcessDataUseCase
from modules.process.app.process.factory import ProcessorFactory
from modules.process.app.process.sms import SmsProcessor
from modules.process.app.process.email import EmailProcessor
from modules.process.app.files.factory import ReaderFileFactory
from modules.process.infrastructure.validators.level_validator import LevelValidator
from modules.process.domain.enums.services import ServiceType
from modules.process.app.confirm.factory import ConfirmFactory
from modules.process.app.confirm.sms import SmsConfirmStrategy
from modules.process.app.confirm.email import EmailConfirmStrategy
from modules.process.app.confirm.call_blasting import CallBlastingConfirmStrategy
from modules.process.infrastructure.storage.local import LocalStorage
from modules.process.domain.constants.cols import Cols

# ---------------------------------------------------------------------------
# Timing collector (session-scoped)
# ---------------------------------------------------------------------------

_timings: list[tuple[str, str, int, float]] = []  # (endpoint, service, rows, ms)


def _record(endpoint: str, service: str, rows: int, elapsed_s: float) -> None:
    _timings.append((endpoint, service, rows, round(elapsed_s * 1000, 1)))


# ---------------------------------------------------------------------------
# Dataset sizes
# ---------------------------------------------------------------------------

SIZES = {
    "small":  2,
    "medium": 100,
    "large":  1_000_000,
}

# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

def _make_sms_csv(path: Path, n: int) -> Path:
    """Write a CSV with n phone numbers valid for Colombia (+57, 10 digits)."""
    phones = [3000000000 + i for i in range(n)]
    lines = ["phone"]
    lines += [str(p) for p in phones]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _make_email_csv(path: Path, n: int) -> Path:
    """Write a CSV with n email addresses."""
    lines = ["email"]
    lines += [f"user{i}@gmail.com" for i in range(n)]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _make_sms_parquet(path: Path, n: int) -> Path:
    """Write a minimal SMS parquet that SmsConfirmStrategy can read."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            Cols.number_concat:   [str(3000000000 + i) for i in range(n)],
            Cols.message:         ["Hola test"] * n,
            Cols.number_operator: ["CLARO"] * n,
            Cols.pdu:             [1] * n,
            Cols.credits:         [0.5] * n,
            Cols.is_ok:           [True] * n,
            Cols.error_code:      [None] * n,
        },
        schema={
            Cols.number_concat:   pl.Utf8,
            Cols.message:         pl.Utf8,
            Cols.number_operator: pl.Utf8,
            Cols.pdu:             pl.Int32,
            Cols.credits:         pl.Float64,
            Cols.is_ok:           pl.Boolean,
            Cols.error_code:      pl.Utf8,
        },
    )
    df.write_parquet(path, compression="zstd")
    return path


def _make_email_parquet(path: Path, n: int) -> Path:
    """Write a minimal Email parquet that EmailConfirmStrategy can read."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            Cols.email:        [f"user{i}@gmail.com" for i in range(n)],
            Cols.message:      ["<p>Hola</p>"] * n,
            Cols.subject:      ["Asunto de prueba"] * n,
            Cols.cost:         [0.02] * n,
            Cols.credits:      [1.0] * n,
            Cols.email_domain: ["gmail"] * n,
            Cols.is_ok:        [True] * n,
            Cols.error_code:   [None] * n,
        },
        schema={
            Cols.email:        pl.Utf8,
            Cols.message:      pl.Utf8,
            Cols.subject:      pl.Utf8,
            Cols.cost:         pl.Float64,
            Cols.credits:      pl.Float64,
            Cols.email_domain: pl.Utf8,
            Cols.is_ok:        pl.Boolean,
            Cols.error_code:   pl.Utf8,
        },
    )
    df.write_parquet(path, compression="zstd")
    return path


# ---------------------------------------------------------------------------
# Mock factories (same pattern as conftest.py)
# ---------------------------------------------------------------------------

def _numeration_mock() -> MagicMock:
    s = np.array([3000000000], dtype=np.int64)
    e = np.array([3099999999], dtype=np.int64)
    o = np.array(["CLARO"], dtype="U50")
    m = MagicMock()
    m.get_ranges = AsyncMock(return_value=(s, e, o))
    return m


def _cost_mock() -> MagicMock:
    m = MagicMock()
    m.get_costs     = AsyncMock(return_value=[("57", 0.5, "COLOMBIA")])
    m.get_costs_cb  = AsyncMock(return_value=[("57", 60.0, "COLOMBIA", 30.0, 15.0)])
    m.get_email_cost = AsyncMock(return_value=0.02)
    return m


def _exclusion_mock(col: str = "phone") -> MagicMock:
    df = pl.DataFrame({col: []}, schema={col: pl.Int64})
    m = MagicMock()
    m.get_df = AsyncMock(return_value=df)
    return m


def _email_exclusion_mock() -> MagicMock:
    df = pl.DataFrame({"email": []}, schema={"email": pl.Utf8})
    m = MagicMock()
    m.get_df = AsyncMock(return_value=df)
    return m


def _sms_repo_mock() -> MagicMock:
    m = MagicMock()
    m.create_campaign_table = AsyncMock(return_value=None)
    m.bulk_insert = AsyncMock(side_effect=lambda cid, df: len(df))
    return m


def _email_repo_mock() -> MagicMock:
    m = MagicMock()
    m.assert_campaigns_exist = AsyncMock(return_value=None)
    m.bulk_insert = AsyncMock(side_effect=lambda cid, df: len(df))
    return m


# ---------------------------------------------------------------------------
# App fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def storage_dir(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("storage")


@pytest.fixture(scope="module")
def test_app(storage_dir: Path) -> FastAPI:
    """Bare FastAPI app (no lifespan) with all external deps mocked."""
    app = FastAPI()
    app.include_router(process_router, prefix="/v2")

    storage = LocalStorage(base_dir=str(storage_dir))

    def _make_use_case() -> ProcessDataUseCase:
        sms_processor = SmsProcessor(
            numeration_service=_numeration_mock(),
            exclusion_source=_exclusion_mock("phone"),
            cost_service=_cost_mock(),
            storage=storage,
        )
        email_processor = EmailProcessor(
            exclusion_source=_email_exclusion_mock(),
            cost_service=_cost_mock(),
            storage=storage,
        )
        return ProcessDataUseCase(
            file_reader_factory=ReaderFileFactory(),
            level_validator=LevelValidator(max_records=10, max_records_elevated=1_000_000),
            processor_factory=ProcessorFactory({
                ServiceType.sms:   sms_processor,
                ServiceType.email: email_processor,
            }),
        )

    def _make_confirm_factory() -> ConfirmFactory:
        sms_repo   = _sms_repo_mock()
        email_repo = _email_repo_mock()
        return ConfirmFactory({
            ServiceType.sms: SmsConfirmStrategy(
                repo=sms_repo, storage=storage
            ),
            ServiceType.email: EmailConfirmStrategy(
                repo=email_repo, storage=storage
            ),
            ServiceType.call_blasting: CallBlastingConfirmStrategy(storage=storage),
        })

    app.dependency_overrides[get_use_case]        = _make_use_case
    app.dependency_overrides[get_confirm_factory] = _make_confirm_factory

    return app


@pytest.fixture
async def client(test_app: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=test_app), base_url="http://test"
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------

def _sms_payload(csv_path: Path, n: int, campaign_id: int = 1) -> dict:
    return {
        "content": "Hola {phone}, su solicitud fue procesada.",
        "shortname": "TEST",
        "tariffId": 1,
        "campaignId": [campaign_id],
        "subService": "standard",
        "useExclusionList": False,
        "configFile": {
            "folder":               str(csv_path),
            "file":                 csv_path.name,
            "delimiter":            ",",
            "useHeaders":           True,
            "nameColumnDemographic": "phone",
            "userIdentifier":       False,
            "nameColumnIdentifier": "",
            "fileRecords":          n,
        },
        "rulesCountry": {
            "idCountry":              57,
            "codeCountry":            57,
            "useCharacterSpecial":    True,
            "limitCharacter":         160,
            "limitCharacterSpecial":  70,
            "numberDigitsMobile":     10,
            "numberDigitsFixed":      7,
            "useShortName":           False,
        },
        "infoUserValidSend": {
            "levelUser":   2,
            "demographic": "",
        },
    }


def _email_payload(csv_path: Path, n: int, campaign_id: int = 2) -> dict:
    return {
        "content": "<p>Hola, este es un correo de prueba.</p>",
        "shortname": "TEST",
        "tariffId": 1,
        "campaignId": [campaign_id],
        "subService": "standard",
        "useExclusionList": False,
        "subject": "Asunto de prueba",
        "configFile": {
            "folder":               str(csv_path),
            "file":                 csv_path.name,
            "delimiter":            ",",
            "useHeaders":           True,
            "nameColumnDemographic": "email",
            "userIdentifier":       False,
            "nameColumnIdentifier": "",
            "fileRecords":          n,
        },
        "rulesCountry": {
            "idCountry":              57,
            "codeCountry":            57,
            "useCharacterSpecial":    False,
            "limitCharacter":         5_000,
            "limitCharacterSpecial":  5_000,
            "numberDigitsMobile":     10,
            "numberDigitsFixed":      7,
            "useShortName":           False,
        },
        "infoUserValidSend": {
            "levelUser":   2,
            "demographic": "",
        },
    }


def _confirm_payload(campaign_id: int) -> dict:
    return {"campaignId": [campaign_id]}


# ---------------------------------------------------------------------------
# /processing/sms
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.parametrize("label,n", list(SIZES.items()))
async def test_processing_sms(label: str, n: int, client: AsyncClient, tmp_path_factory):
    csv_path = tmp_path_factory.mktemp(f"sms_{label}") / "sms.csv"
    _make_sms_csv(csv_path, n)
    payload = _sms_payload(csv_path, n, campaign_id=100 + n)

    t0 = time.perf_counter()
    resp = await client.post("/v2/processing/sms", json=payload)
    elapsed = time.perf_counter() - t0

    _record("/processing", "sms", n, elapsed)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"] is True
    general = body["summaryGeneral"]
    assert general["total_records"] == n
    assert general["total_excluded"] == 0


# ---------------------------------------------------------------------------
# /processing/email
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.parametrize("label,n", list(SIZES.items()))
async def test_processing_email(label: str, n: int, client: AsyncClient, tmp_path_factory):
    csv_path = tmp_path_factory.mktemp(f"email_{label}") / "email.csv"
    _make_email_csv(csv_path, n)
    payload = _email_payload(csv_path, n, campaign_id=200 + n)

    t0 = time.perf_counter()
    resp = await client.post("/v2/processing/email", json=payload)
    elapsed = time.perf_counter() - t0

    _record("/processing", "email", n, elapsed)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"] is True
    general = body["summaryGeneral"]
    assert general["total_records"] == n
    assert general["total_excluded"] == 0


# ---------------------------------------------------------------------------
# /confirm/sms
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.parametrize("label,n", list(SIZES.items()))
async def test_confirm_sms(label: str, n: int, client: AsyncClient, storage_dir: Path):
    campaign_id = 300 + n
    parquet_path = storage_dir / f"Campaign/sms/campaign_{campaign_id}.parquet"
    _make_sms_parquet(parquet_path, n)
    payload = _confirm_payload(campaign_id)

    t0 = time.perf_counter()
    resp = await client.post("/v2/confirm/sms", json=payload)
    elapsed = time.perf_counter() - t0

    _record("/confirm", "sms", n, elapsed)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "inserted" in body
    assert body["inserted"] == n


# ---------------------------------------------------------------------------
# /confirm/email
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.parametrize("label,n", list(SIZES.items()))
async def test_confirm_email(label: str, n: int, client: AsyncClient, storage_dir: Path):
    campaign_id = 400 + n
    parquet_path = storage_dir / f"Campaign/email/campaign_{campaign_id}.parquet"
    _make_email_parquet(parquet_path, n)
    payload = _confirm_payload(campaign_id)

    t0 = time.perf_counter()
    resp = await client.post("/v2/confirm/email", json=payload)
    elapsed = time.perf_counter() - t0

    _record("/confirm", "email", n, elapsed)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "inserted" in body
    assert body["inserted"] == n


# ---------------------------------------------------------------------------
# Error-case sanity checks
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_processing_invalid_service(client: AsyncClient):
    resp = await client.post("/v2/processing/fax", json={})
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_confirm_file_not_found(client: AsyncClient):
    resp = await client.post("/v2/confirm/sms", json={"campaignId": [99999]})
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /confirm/sms — REAL DATABASE (campaign 99999191)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_confirm_sms_real_db():
    """
    Crea la tabla campana_99999191 en telefonos_campanas e inserta
    1 000 000 de registros usando SmsConfirmRepository real.

    - Crea sesión async real con DB_TELEFONOS_CAMPANAS
    - create_campaign_table → CREATE TABLE IF NOT EXISTS campana_99999191
    - bulk_insert → LOAD DATA LOCAL INFILE (>= 50k threshold)
    """
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from config.settings import settings
    from modules.process.infrastructure.repositories.sms_confirm import SmsConfirmRepository

    CAMPAIGN_ID = 99999191
    N_ROWS = 1_000_000

    df = pl.DataFrame(
        {
            "celular":        [str(3000000000 + i) for i in range(N_ROWS)],
            "id_campana":     [CAMPAIGN_ID] * N_ROWS,
            "estado":         ["P"] * N_ROWS,
            "texto":          ["Hola test"] * N_ROWS,
            "identificacion": [""] * N_ROWS,
            "servicio":       [str((i % 100) + 1) for i in range(N_ROWS)],
            "operador":       ["CLARO"] * N_ROWS,
            "pdu":            [1] * N_ROWS,
            "credit":         [0.5] * N_ROWS,
        }
    )

    engine = create_async_engine(settings.DB_TELEFONOS_CAMPANAS, pool_pre_ping=True)
    AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    try:
        async with AsyncSessionLocal() as session:
            repo = SmsConfirmRepository(session=session, sync_dsn=settings.DB_TELEFONOS_CAMPANAS)
            await repo.create_campaign_table(CAMPAIGN_ID)

            t0 = time.perf_counter()
            inserted = await repo.bulk_insert(CAMPAIGN_ID, df)
            elapsed = time.perf_counter() - t0
    finally:
        await engine.dispose()

    _record("/confirm (real DB)", "sms", N_ROWS, elapsed)

    db_host = settings.DB_TELEFONOS_CAMPANAS.split("@")[-1].split("/")[0] if "@" in settings.DB_TELEFONOS_CAMPANAS else "configured host"

    print(f"\n")
    print(f"  ┌─ INSERCIÓN REAL EN BASE DE DATOS — SMS ───────────────────────┐")
    print(f"  │  Host        : {db_host}")
    print(f"  │  Base        : telefonos_campanas")
    print(f"  │  Tabla       : campana_{CAMPAIGN_ID}")
    print(f"  │  id_campana  : {CAMPAIGN_ID}")
    print(f"  │  Filas       : {inserted:,}")
    print(f"  │  Tiempo      : {elapsed * 1000:.1f} ms")
    print(f"  └────────────────────────────────────────────────────────────────┘")

    assert inserted == N_ROWS


# ---------------------------------------------------------------------------
# /confirm/email — REAL DATABASE (tabla mail_test)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_confirm_email_real_db():
    """
    Inserta 1 000 000 registros en mail_campaings.mail_test con
    id_campaign=999999991.

    Usa LOAD DATA LOCAL INFILE directamente (mismo mecanismo que
    EmailConfirmRepository) apuntando a mail_test en vez de mail.
    Crea mail_test si no existe (mismo esquema que mail).
    """
    import asyncio
    import os
    import re
    import tempfile
    from sqlalchemy import create_engine, text
    from config.settings import settings

    CAMPAIGN_ID = 999999991
    N_ROWS = 1_000_000
    TARGET_TABLE = "mail_test"

    _CREATE_MAIL_TEST = f"""
        CREATE TABLE IF NOT EXISTS `mail_campaings`.`{TARGET_TABLE}` (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            mail        VARCHAR(255) NOT NULL,
            id_campaign INT NOT NULL,
            id_client   VARCHAR(250),
            status      VARCHAR(1) NOT NULL,
            body        TEXT NOT NULL,
            services    VARCHAR(3) NOT NULL,
            subject     TEXT,
            name_client VARCHAR(250)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    df = pl.DataFrame(
        {
            "mail":        [f"user{i}@gmail.com" for i in range(N_ROWS)],
            "body":        ["<p>Hola</p>"] * N_ROWS,
            "subject":     ["Asunto de prueba"] * N_ROWS,
            "id_client":   [""] * N_ROWS,
            "name_client": [""] * N_ROWS,
            "status":      ["P"] * N_ROWS,
            "services":    [str((i % 100) + 1) for i in range(N_ROWS)],
        }
    )

    sync_dsn = re.sub(r"^mysql\+\w+://", "mysql+pymysql://", settings.DB_EMAIL)

    def _run() -> int:
        engine = create_engine(sync_dsn, connect_args={"local_infile": True})
        try:
            with engine.begin() as conn:
                conn.execute(text(_CREATE_MAIL_TEST))

            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
            os.close(tmp_fd)
            try:
                columns_str = ", ".join(f"`{c}`" for c in df.columns)
                df.write_csv(tmp_path, separator="\t", include_header=False, null_value="")
                safe_path = tmp_path.replace("\\", "/")
                load_sql = (
                    f"LOAD DATA LOCAL INFILE '{safe_path}' "
                    f"INTO TABLE `mail_campaings`.`{TARGET_TABLE}` "
                    f"CHARACTER SET utf8mb4 "
                    f"FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' "
                    f"LINES TERMINATED BY '\\n' ({columns_str}) "
                    f"SET id_campaign = {CAMPAIGN_ID}"
                )
                with engine.begin() as conn:
                    conn.execute(text(load_sql))
            finally:
                os.unlink(tmp_path)
        finally:
            engine.dispose()
        return df.height

    t0 = time.perf_counter()
    inserted = await asyncio.to_thread(_run)
    elapsed = time.perf_counter() - t0

    _record("/confirm (real DB)", "email", N_ROWS, elapsed)

    db_host = settings.DB_EMAIL.split("@")[-1].split("/")[0] if "@" in settings.DB_EMAIL else "configured host"

    print(f"\n")
    print(f"  ┌─ INSERCIÓN REAL EN BASE DE DATOS — EMAIL ─────────────────────┐")
    print(f"  │  Host        : {db_host}")
    print(f"  │  Base        : mail_campaings")
    print(f"  │  Tabla       : mail_campaings.{TARGET_TABLE}")
    print(f"  │  id_campaign : {CAMPAIGN_ID}")
    print(f"  │  Filas       : {inserted:,}")
    print(f"  │  Tiempo      : {elapsed * 1000:.1f} ms")
    print(f"  └────────────────────────────────────────────────────────────────┘")

    assert inserted == N_ROWS


# ---------------------------------------------------------------------------
# Timing table (printed once at session end via autouse fixture)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def print_timing_table():
    yield
    if not _timings:
        return

    print("\n")
    print("=" * 70)
    print("  EXECUTION TIME TABLE")
    print("=" * 70)
    header = f"{'Endpoint':<16} {'Service':<8} {'Rows':>6}  {'Time (ms)':>10}"
    print(header)
    print("-" * 70)

    sorted_timings = sorted(_timings, key=lambda r: (r[0], r[1], r[2]))
    for endpoint, service, rows, ms in sorted_timings:
        label = "small" if rows <= 2 else ("medium" if rows <= 100 else "large")
        print(f"{endpoint:<16} {service:<8} {rows:>6} ({label:<6})  {ms:>8.1f} ms")

    print("=" * 70)
