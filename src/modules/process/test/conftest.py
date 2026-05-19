"""
Fixtures y helpers compartidos para todos los tests del módulo process.

- Fuerza backend asyncio (trio no está instalado).
- Expone make_ctx(), make_excl_config(), storage y mocks reutilizables.
"""
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import polars as pl
import pytest

from modules.process.domain.models.process_dto import (
    ConfigFile,
    ConfigListExclusion,
    DataProcessingDTO,
    InfoUserValidSend,
    RulesCountry,
)
from modules.process.domain.interfaces.storage import IStorage

# ── forzar asyncio únicamente ─────────────────────────────────────────────────

@pytest.fixture(params=["asyncio"])
def anyio_backend(request):
    return request.param


# ── rutas ─────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parents[4]
DATA_FILE    = PROJECT_ROOT / "files" / "data.csv"
RESULTS_DIR  = PROJECT_ROOT / "resultados" / "test_process"

# Activa el guardado de resultados en RESULTS_DIR.
# Por defecto desactivado; ejecutar con SAVE_TEST_RESULTS=1 pytest para activar.
_SAVE_RESULTS = os.getenv("SAVE_TEST_RESULTS", "0") == "1"

# Directorio temporal compartido por toda la sesión de pytest cuando el
# guardado permanente está desactivado.
_TMP_DIR = Path(tempfile.mkdtemp(prefix="pytest_process_")) if not _SAVE_RESULTS else None


def _out_dir(scenario: str) -> Path:
    """Devuelve el directorio de salida según el modo activo."""
    if _SAVE_RESULTS:
        path = RESULTS_DIR / scenario
    else:
        path = _TMP_DIR / scenario  # type: ignore[operator]
    path.mkdir(parents=True, exist_ok=True)
    return path


# ── storage de análisis ───────────────────────────────────────────────────────

class AnalysisStorage(IStorage):
    """
    Guarda parquet en disco para que los tests puedan releerlo.

    Cuando SAVE_TEST_RESULTS=1 los archivos van a RESULTS_DIR/<scenario>/
    (útil para análisis offline). En caso contrario van a un directorio
    temporal que se descarta al terminar el proceso.
    """

    def __init__(self, scenario: str):
        self.out = _out_dir(scenario)
        self.saved_dfs: list[pl.DataFrame]  = []
        self.saved_paths: list[str]         = []

    async def save(self, df: pl.DataFrame, filename: str) -> str:
        path = self.out / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path, compression="zstd")
        if _SAVE_RESULTS:
            df.write_csv(self.out / filename.replace(".parquet", ".csv"))
        self.saved_dfs.append(df)
        self.saved_paths.append(str(path))
        return str(path)

    def last_df(self) -> pl.DataFrame:
        return self.saved_dfs[-1]


def save_summary(scenario: str, result: dict) -> None:
    """Persiste el resumen JSON solo cuando SAVE_TEST_RESULTS=1."""
    if not _SAVE_RESULTS:
        return
    path = RESULTS_DIR / scenario / "summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, default=str))


# ── DTO helpers ───────────────────────────────────────────────────────────────

BASE_RULES_SMS = RulesCountry(
    idCountry=57,
    codeCountry=57,
    numberDigitsMobile=10,
    numberDigitsFixed=7,
    useCharacterSpecial=True,
    limitCharacter=160,
    limitCharacterSpecial=70,
    useShortName=False,
)

BASE_RULES_CHILE = RulesCountry(
    idCountry=56,
    codeCountry=56,
    numberDigitsMobile=9,
    numberDigitsFixed=8,
    useCharacterSpecial=True,
    limitCharacter=160,
    limitCharacterSpecial=70,
    useShortName=False,
)


def make_config_file(
    path: str = str(DATA_FILE),
    demographic: str = "phone",
    delimiter: str = ";",
) -> ConfigFile:
    return ConfigFile(
        folder=path,
        file=Path(path).name,
        delimiter=delimiter,
        useHeaders=True,
        nameColumnDemographic=demographic,
        userIdentifier=False,
        nameColumnIdentifier="",
        fileRecords=100,
    )


def make_excl_config(
    path: str = str(DATA_FILE),
    demographic: str = "phone",
) -> ConfigListExclusion:
    return ConfigListExclusion(
        folder=path,
        file=Path(path).name,
        delimiter=";",
        useHeaders=True,
        nameColumnDemographic=demographic,
    )


def make_ctx(
    content:         str   = "Mensaje de prueba.",
    shortname:       str | None = None,
    sub_service:     str   = "standard",
    demographic:     str   = "phone",
    rules:           RulesCountry | None = None,
    use_exclusion:   bool  = False,
    excl_config:     ConfigListExclusion | None = None,
    audio_duration:  float | None = None,
    audio_path:      str   | None = None,
    tariff_id:       int   = 1,
    campaign_id:     list[int] | None = None,
    code_group:      str   = "test_grp_001",
    subject:         str   | None = None,
) -> DataProcessingDTO:
    return DataProcessingDTO(
        content=content,
        shortname=shortname,
        tariffId=tariff_id,
        campaignId=campaign_id or [],
        codeGroup=code_group,
        subService=sub_service,
        useExclusionList=use_exclusion,
        configListExclusion=excl_config,
        configFile=make_config_file(demographic=demographic),
        rulesCountry=rules or BASE_RULES_SMS,
        infoUserValidSend=InfoUserValidSend(levelUser=2, demographic=""),
        audioDuration=audio_duration,
        audioPath=audio_path,
        subject=subject,
    )


# ── mock factories ────────────────────────────────────────────────────────────

def numeration_mock(
    starts:    list[int] | None = None,
    ends:      list[int] | None = None,
    operators: list[str] | None = None,
):
    """Rangos de Colombia que cubren los números de data.csv."""
    s = np.array(starts or [3000000000, 3200000000], dtype=np.int64)
    e = np.array(ends   or [3099999999, 3299999999], dtype=np.int64)
    o = np.array(operators or ["CLARO", "MOVISTAR"], dtype="U50")
    mock = MagicMock()
    mock.get_ranges = AsyncMock(return_value=(s, e, o))
    return mock


def cost_mock(costs: list[tuple] | None = None):
    data = costs or [("57", 0.5, "COLOMBIA")]
    mock = MagicMock()
    mock.get_costs = AsyncMock(return_value=data)
    return mock


def cb_cost_mock(rows: list[tuple] | None = None):
    """Devuelve (prefix, cost, operator, initial, incremental)."""
    data = rows or [("57", 60.0, "COLOMBIA", 30.0, 15.0)]
    mock = MagicMock()
    mock.get_costs_cb = AsyncMock(return_value=data)
    return mock


def email_cost_mock(cost: float | None = None):
    """Devuelve un costo plano para email."""
    mock = MagicMock()
    mock.get_email_cost = AsyncMock(return_value=cost if cost is not None else 0.02)
    return mock


def exclusion_mock(numbers: list[int] | None = None, col: str = "phone"):
    df = pl.DataFrame({col: numbers or []}, schema={col: pl.Int64})
    mock = MagicMock()
    mock.get_df = AsyncMock(return_value=df)
    return mock


def email_exclusion_mock(emails: list[str] | None = None, col: str = "email"):
    df = pl.DataFrame({col: emails or []}, schema={col: pl.Utf8})
    mock = MagicMock()
    mock.get_df = AsyncMock(return_value=df)
    return mock


def duration_mock(seconds: float = 30.0):
    mock = MagicMock()
    mock.get_duration = AsyncMock(return_value=seconds)
    return mock


# ── helpers de DataFrame ──────────────────────────────────────────────────────

def base_phone_df(numbers: list[int] | None = None) -> pl.DataFrame:
    """DataFrame de teléfonos ya normalizados (post-CleanData), con is_ok/error_code."""
    from modules.process.domain.constants.cols import Cols
    nums = numbers or [3005973563, 3208392650]
    return pl.DataFrame(
        {"phone": nums, Cols.is_ok: [True] * len(nums), Cols.error_code: [None] * len(nums)},
        schema={"phone": pl.Int64, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
    )


def base_email_df(emails: list[str] | None = None) -> pl.DataFrame:
    """DataFrame de emails ya normalizados (post-CleanDataEmail), con is_ok/error_code."""
    from modules.process.domain.constants.cols import Cols
    addrs = emails or ["user@gmail.com", "contact@hotmail.com"]
    return pl.DataFrame(
        {Cols.email: addrs, Cols.is_ok: [True] * len(addrs), Cols.error_code: [None] * len(addrs)},
        schema={Cols.email: pl.Utf8, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
    )
