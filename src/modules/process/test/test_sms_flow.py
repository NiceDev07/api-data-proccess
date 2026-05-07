"""
Integration test for the full SMS pipeline.

Runs SmsProcessor end-to-end against files/data.csv with mocked external
services (numeration, cost, exclusion). Uses conftest fixtures so no files
are written to disk unless SAVE_TEST_RESULTS=1 is set.
"""

from pathlib import Path

import polars as pl
import pytest

from modules.process.app.files.csv_reader import CsvReader
from modules.process.app.process.sms import SmsProcessor
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason
from modules.process.domain.models.process_dto import (
    ConfigFile,
    ConfigListExclusion,
    DataProcessingDTO,
    InfoUserValidSend,
    SmsDataProcessingDTO,
)

from .conftest import (
    AnalysisStorage,
    BASE_RULES_SMS,
    cost_mock,
    exclusion_mock,
    numeration_mock,
    save_summary,
)

PROJECT_ROOT = Path(__file__).parents[4]
DATA_FILE = PROJECT_ROOT / "files" / "data.csv"

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
        rulesCountry=BASE_RULES_SMS,
        infoUserValidSend=InfoUserValidSend(levelUser=2, demographic=""),
    )
    defaults.update(overrides)
    return DataProcessingDTO(**defaults)


async def read_df(payload: DataProcessingDTO) -> pl.DataFrame:
    df = await CsvReader().read(payload.configFile)
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
    scenario = "sms_flow/happy_path"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    result = await processor.process(df, payload)
    save_summary(scenario, result)

    assert result["success"] is True

    general = result["summaryGeneral"]
    assert general["total_records"] == 2
    assert general["total_excluded"] == 0
    assert general["total_pdu"] == 2
    assert abs(general["total_credits"] - 1.0) < 1e-6

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
    scenario = "sms_flow/exclusion_list"
    excluded_number = 3005973563

    excl_config = ConfigListExclusion(
        folder=str(DATA_FILE),
        file="data.csv",
        delimiter=";",
        useHeaders=True,
        nameColumnDemographic="number",
    )

    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(numbers=[excluded_number], col="number"),
        cost_service=cost_mock(),
        storage=storage,
    )

    payload = make_payload(useExclusionList=True, configListExclusion=excl_config)
    df = await read_df(payload)
    result = await processor.process(df, payload)
    save_summary(scenario, result)

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
    """Second number falls outside all operator ranges → excluded with NO_OPERATOR."""
    scenario = "sms_flow/no_operator_partial"

    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(
            starts=[3000000000], ends=[3099999999], operators=["CLARO"]
        ),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock([("57", 0.5, "COLOMBIA")]),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    result = await processor.process(df, payload)
    save_summary(scenario, result)

    general = result["summaryGeneral"]
    assert general["total_records"] == 1
    assert general["total_excluded"] == 1

    nok = storage.last_df().filter(~pl.col(Cols.is_ok))
    assert nok[Cols.error_code][0] == ExclusionReason.NO_OPERATOR


@pytest.mark.anyio
async def test_sms_flow_custom_message_tag():
    """Template tag {nombre} is replaced with data from the CSV column."""
    scenario = "sms_flow/custom_message_tag"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(),
        storage=storage,
    )

    payload = make_payload(content="Estimado {nombre}, su solicitud fue procesada.")
    df = await read_df(payload)
    result = await processor.process(df, payload)
    save_summary(scenario, result)

    saved = storage.last_df()
    messages = saved[Cols.message].to_list()
    assert "esteban xde" in messages[0]
    assert "otro" in messages[1]
    assert result["success"] is True


@pytest.mark.anyio
async def test_sms_flow_char_limit_exceeded():
    """Messages exceeding limitCharacter are marked is_ok=False with CHAR_LIMIT_EXCEEDED."""
    from modules.process.domain.models.process_dto import RulesCountry

    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(),
        storage=AnalysisStorage("sms_flow/char_limit_exceeded"),
    )

    rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "limitCharacter": 160, "useCharacterSpecial": False})
    payload = make_payload(content="A" * 161, rulesCountry=rules)
    df = await read_df(payload)
    result = await processor.process(df, payload)

    violations = result.get("violations", [])
    assert any(v["code"] == ExclusionReason.CHAR_LIMIT_EXCEEDED for v in violations), (
        f"Se esperaba violación CHAR_LIMIT_EXCEEDED en violations: {violations}"
    )


@pytest.mark.anyio
async def test_sms_flow_shortname_required():
    """useShortName=True and shortname absent marks records is_ok=False with SHORTNAME_MISSING."""
    from modules.process.domain.models.process_dto import RulesCountry

    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(),
        storage=AnalysisStorage("sms_flow/shortname_required"),
    )

    rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": True})
    payload = make_payload(content="Mensaje sin shortname.", rulesCountry=rules)
    df = await read_df(payload)
    result = await processor.process(df, payload)

    violations = result.get("violations", [])
    assert any(v["code"] == ExclusionReason.SHORTNAME_MISSING for v in violations), (
        f"Se esperaba violación SHORTNAME_MISSING en violations: {violations}"
    )


@pytest.mark.anyio
async def test_sms_flow_result_parquet_saved():
    """Ensures the parquet file is accessible with the expected columns."""
    scenario = "sms_flow/result_parquet"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    await processor.process(df, payload)

    assert len(storage.saved_dfs) == 1
    saved = storage.last_df()
    assert Cols.number_concat in saved.columns
    assert Cols.message in saved.columns
    assert Cols.pdu in saved.columns
    assert Cols.credits in saved.columns
    assert Cols.is_ok in saved.columns
    assert len(saved) == 2


# ---------------------------------------------------------------------------
# Fix 1 — shortname vacío rechazado por validación Pydantic
# ---------------------------------------------------------------------------

def test_sms_shortname_empty_rejected():
    """SmsDataProcessingDTO rechaza shortname vacío o solo espacios."""
    from pydantic import ValidationError

    base = make_payload(shortname="SAEM3").model_dump()

    for valor_invalido in ["", "   "]:
        base["shortname"] = valor_invalido
        with pytest.raises(ValidationError) as exc_info:
            SmsDataProcessingDTO.model_validate(base)
        errores = exc_info.value.errors()
        assert any(e["loc"] == ("shortname",) for e in errores), (
            f"Se esperaba error en campo 'shortname' para valor '{valor_invalido}'"
        )
