"""
Integration test for the full SMS pipeline.

Runs SmsProcessor end-to-end against files/data.csv with mocked external
services (numeration, cost, exclusion). Results are persisted in
resultados/test_sms_flow/<scenario>/ for offline analysis.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import polars as pl
import pytest

from modules.process.app.files.csv_reader import CsvReader
from modules.process.app.process.sms import SmsProcessor
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason
from modules.process.domain.interfaces.storage import IStorage
from modules.process.domain.models.process_dto import (
    ConfigFile,
    ConfigListExclusion,
    DataProcessingDTO,
    InfoUserValidSend,
    RulesCountry,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parents[4]
DATA_FILE = PROJECT_ROOT / "files" / "data.csv"
RESULTS_DIR = PROJECT_ROOT / "resultados" / "test_sms_flow"

# ---------------------------------------------------------------------------
# Storage that saves results for analysis
# ---------------------------------------------------------------------------


class AnalysisStorage(IStorage):
    """Saves parquet + human-readable CSV to RESULTS_DIR/<scenario>/."""

    def __init__(self, scenario: str):
        self.out = RESULTS_DIR / scenario
        self.out.mkdir(parents=True, exist_ok=True)
        self.saved_dfs: list[pl.DataFrame] = []
        self.saved_paths: list[str] = []

    async def save(self, df: pl.DataFrame, filename: str) -> str:
        path = self.out / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path, compression="zstd")
        df.write_csv(self.out / filename.replace(".parquet", ".csv"))
        self.saved_dfs.append(df)
        self.saved_paths.append(str(path))
        return str(path)

    def last_df(self) -> pl.DataFrame:
        return self.saved_dfs[-1]


# ---------------------------------------------------------------------------
# Mock factories
# ---------------------------------------------------------------------------


def numeration_mock(
    starts: list[int] | None = None,
    ends: list[int] | None = None,
    operators: list[str] | None = None,
):
    """Colombia operator ranges covering data.csv numbers (3005973563, 3208392650).
    dtype must be fixed-width string (not object) so polars can write parquet.
    """
    s = np.array(starts or [3000000000, 3200000000], dtype=np.int64)
    e = np.array(ends or [3099999999, 3299999999], dtype=np.int64)
    o = np.array(operators or ["CLARO", "MOVISTAR"], dtype="U50")
    mock = MagicMock()
    mock.get_ranges = AsyncMock(return_value=(s, e, o))
    return mock


def cost_mock(costs: list[tuple] | None = None):
    """Prefix-based cost for Colombia (+57) numbers.
    After ConcatPrefix: 573005973563, 573208392650 → both start with '57'.
    """
    data = costs if costs is not None else [("57", 0.5, "COLOMBIA")]
    mock = MagicMock()
    mock.get_costs = AsyncMock(return_value=data)
    return mock


def exclusion_mock(numbers: list[int] | None = None, col: str = "number"):
    """Returns a DF with the given numbers as the exclusion set (empty by default)."""
    df = pl.DataFrame({col: numbers or []}, schema={col: pl.Int64})
    mock = MagicMock()
    mock.get_df = AsyncMock(return_value=df)
    return mock


# ---------------------------------------------------------------------------
# DTO helpers
# ---------------------------------------------------------------------------


BASE_RULES = RulesCountry(
    idCountry=57,           # Colombia
    codeCountry=57,         # +57 prefix
    numberDigitsMobile=10,  # Colombian mobile numbers are 10 digits
    numberDigitsFixed=7,
    useCharacterSpecial=True,
    limitCharacter=160,
    limitCharacterSpecial=70,
    useShortName=False,
)

BASE_CONFIG_FILE = ConfigFile(
    folder=str(DATA_FILE),
    file="data.csv",
    delimiter=";",
    useHeaders=True,
    nameColumnDemographic="number",
    userIdentifier=False,
    nameColumnIdentifier="",
    fileRecords=2,
)


def make_payload(**overrides) -> DataProcessingDTO:
    defaults = dict(
        content="Hola, este es un mensaje SMS de prueba.",
        shortname="SAEM3",
        tariffId=1,
        campaignId=[999],
        subService="standard",
        useExclusionList=False,
        configFile=BASE_CONFIG_FILE,
        rulesCountry=BASE_RULES,
        infoUserValidSend=InfoUserValidSend(levelUser=2, demographic=""),
    )
    defaults.update(overrides)
    return DataProcessingDTO(**defaults)


async def read_df(payload: DataProcessingDTO) -> pl.DataFrame:
    df = await CsvReader().read(payload.configFile)
    # NOTE: No pipeline currently creates __IDENTIFIER__. The production use case
    # will need an AssignIdentifier step. For now we simulate that step here so
    # SaveResults can select the column without failing.
    if payload.configFile.userIdentifier and payload.configFile.nameColumnIdentifier:
        df = df.with_columns(
            pl.col(payload.configFile.nameColumnIdentifier)
            .cast(pl.Utf8)
            .alias(Cols.identifier)
        )
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(Cols.identifier))
    return df


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_sms_flow_happy_path():
    """Both records pass every stage and produce a valid summary."""
    scenario = "happy_path"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(),
        cost_service=cost_mock(),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    result = await processor.process(df, payload)

    _save_summary(scenario, result)

    assert result["success"] is True

    general = result["summaryGeneral"]
    assert general["total_records"] == 2
    assert general["total_excluded"] == 0
    assert general["total_pdu"] == 2        # 1 PDU each (40-char ASCII message)
    assert abs(general["total_credits"] - 1.0) < 1e-6

    # cost_mock uses a single "COLOMBIA" prefix → one group
    groups = result["summaryGroup"]
    assert len(groups) == 1
    grp = groups[0]
    assert grp["operator"] == "COLOMBIA"
    assert grp["total"] == 2
    assert grp["pdu"] == 2
    assert abs(grp["unit_value"] - 0.5) < 1e-6

    saved = storage.last_df()
    assert len(saved) == 2
    assert saved[Cols.is_ok].all()
    assert saved[Cols.error_code].is_null().all()
    assert (saved[Cols.pdu] == 1).all()
    assert (saved[Cols.credits] == 0.5).all()


@pytest.mark.anyio
async def test_sms_flow_exclusion_list():
    """One number is in the exclusion list → marked excluded, one remains valid."""
    scenario = "exclusion_list"
    excluded_number = 3005973563   # first row in data.csv (national number after CleanData)

    storage = AnalysisStorage(scenario)

    excl_config = ConfigListExclusion(
        folder=str(DATA_FILE),  # not actually read; source is mocked
        file="data.csv",
        delimiter=";",
        useHeaders=True,
        nameColumnDemographic="number",
    )

    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(numbers=[excluded_number]),
        cost_service=cost_mock(),
        storage=storage,
    )

    payload = make_payload(useExclusionList=True, configListExclusion=excl_config)
    df = await read_df(payload)
    result = await processor.process(df, payload)

    _save_summary(scenario, result)

    general = result["summaryGeneral"]
    assert general["total_records"] == 1
    assert general["total_excluded"] == 1

    saved = storage.last_df()
    assert len(saved) == 2

    ok = saved.filter(pl.col(Cols.is_ok))
    nok = saved.filter(~pl.col(Cols.is_ok))

    assert len(ok) == 1
    assert len(nok) == 1
    assert nok[Cols.error_code][0] == ExclusionReason.EXCLUSION_LIST


@pytest.mark.anyio
async def test_sms_flow_no_operator_partial():
    """Second number falls outside all operator ranges → excluded with NO_OPERATOR.
    First number is valid so _build_summary does not raise.
    """
    scenario = "no_operator_partial"

    # Covers 3005973563 (CLARO range) but NOT 3208392650 (outside range)
    starts = [3000000000]
    ends   = [3099999999]
    ops    = ["CLARO"]

    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(starts, ends, ops),
        exclusion_source=exclusion_mock(),
        cost_service=cost_mock([("57", 0.5, "COLOMBIA")]),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    result = await processor.process(df, payload)

    _save_summary(scenario, result)

    general = result["summaryGeneral"]
    assert general["total_records"] == 1
    assert general["total_excluded"] == 1

    saved = storage.last_df()
    nok = saved.filter(~pl.col(Cols.is_ok))
    assert nok[Cols.error_code][0] == ExclusionReason.NO_OPERATOR


@pytest.mark.anyio
async def test_sms_flow_custom_message_tag():
    """Template tag {nombre} is replaced with data from the CSV column."""
    scenario = "custom_message_tag"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(),
        cost_service=cost_mock(),
        storage=storage,
    )

    # {nombre} tag → replaced by the 'nombre' column in data.csv
    payload = make_payload(content="Estimado {nombre}, su solicitud fue procesada.")
    df = await read_df(payload)
    result = await processor.process(df, payload)

    _save_summary(scenario, result)

    saved = storage.last_df()
    messages = saved[Cols.message].to_list()
    # 'nombre' column in data.csv has "esteban xde" and "otro"
    assert "esteban xde" in messages[0]
    assert "otro" in messages[1]
    assert result["success"] is True


@pytest.mark.anyio
async def test_sms_flow_char_limit_exceeded():
    """Messages exceeding limitCharacter are marked is_ok=False with CHAR_LIMIT_EXCEEDED."""
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(),
        cost_service=cost_mock(),
        storage=AnalysisStorage("char_limit_exceeded"),
    )

    long_content = "A" * 161   # 1 char over the 160-char limit
    rules = RulesCountry(
        **{**BASE_RULES.model_dump(), "limitCharacter": 160, "useCharacterSpecial": False}
    )
    payload = make_payload(content=long_content, rulesCountry=rules)
    df = await read_df(payload)

    result = await processor.process(df, payload)

    violations = result.get("violations", [])
    assert any(v["code"] == ExclusionReason.CHAR_LIMIT_EXCEEDED for v in violations), (
        f"Se esperaba violación CHAR_LIMIT_EXCEEDED en violations: {violations}"
    )


@pytest.mark.anyio
async def test_sms_flow_shortname_required():
    """useShortName=True and shortname absent marks records is_ok=False with SHORTNAME_MISSING."""
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(),
        cost_service=cost_mock(),
        storage=AnalysisStorage("shortname_required"),
    )

    rules = RulesCountry(**{**BASE_RULES.model_dump(), "useShortName": True})
    # content does NOT include the shortname "SAEM3"
    payload = make_payload(content="Mensaje sin shortname.", rulesCountry=rules)
    df = await read_df(payload)

    result = await processor.process(df, payload)

    violations = result.get("violations", [])
    assert any(v["code"] == ExclusionReason.SHORTNAME_MISSING for v in violations), (
        f"Se esperaba violación SHORTNAME_MISSING en violations: {violations}"
    )


@pytest.mark.anyio
async def test_sms_flow_result_parquet_saved():
    """Ensures the parquet file exists on disk with the expected columns."""
    scenario = "result_parquet"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(),
        cost_service=cost_mock(),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    await processor.process(df, payload)

    assert len(storage.saved_paths) == 1
    parquet_path = Path(storage.saved_paths[0])
    assert parquet_path.exists()
    assert parquet_path.suffix == ".parquet"

    loaded = pl.read_parquet(parquet_path)
    assert Cols.number_concat in loaded.columns
    assert Cols.message in loaded.columns
    assert Cols.pdu in loaded.columns
    assert Cols.credits in loaded.columns
    assert Cols.is_ok in loaded.columns
    assert len(loaded) == 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _save_summary(scenario: str, result: dict) -> None:
    path = RESULTS_DIR / scenario / "summary.json"
    path.write_text(json.dumps(result, indent=2, default=str))
