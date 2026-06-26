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

from modules.process.test.conftest import (
    AnalysisStorage,
    BASE_RULES_SMS,
    cost_mock,
    exclusion_mock,
    numeration_mock,
    save_summary,
)

PROJECT_ROOT = Path(__file__).parents[5]
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
        codeGroup="test_sms_0999",
        subService="standard",
        useExclusionList=False,
        configFile=BASE_CONFIG_FILE,
        rulesCountry=BASE_RULES_SMS,
        infoUserValidSend=InfoUserValidSend(levelUser=2, demographic=""),
    )
    defaults.update(overrides)
    return DataProcessingDTO(**defaults)


async def read_df(payload: DataProcessingDTO) -> pl.DataFrame:
    # Solo lee el archivo; el processor adjunta Cols.identifier internamente
    return await CsvReader().read(payload.configFile)


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


# test_sms_flow_char_limit_exceeded — DESACTIVADO. CharLimitRegulation está
# comentada porque el flujo activo del data-process anterior no rechazaba
# mensajes largos: CalculatePDU los cobra como multi-parte.
#
# @pytest.mark.anyio
# async def test_sms_flow_char_limit_exceeded():
#     """Messages exceeding limitCharacter are marked is_ok=False with CHAR_LIMIT_EXCEEDED."""
#     from modules.process.domain.models.process_dto import RulesCountry
#
#     processor = SmsProcessor(
#         numeration_service=numeration_mock(),
#         exclusion_source=exclusion_mock(col="number"),
#         cost_service=cost_mock(),
#         storage=AnalysisStorage("sms_flow/char_limit_exceeded"),
#     )
#
#     rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "limitCharacter": 160, "useCharacterSpecial": False})
#     payload = make_payload(content="A" * 161, rulesCountry=rules)
#     df = await read_df(payload)
#     result = await processor.process(df, payload)
#
#     violations = result.get("violations", [])
#     assert any(v["code"] == ExclusionReason.CHAR_LIMIT_EXCEEDED for v in violations), (
#         f"Se esperaba violación CHAR_LIMIT_EXCEEDED en violations: {violations}"
#     )


@pytest.mark.anyio
async def test_sms_flow_shortname_missing_aborts_campaign():
    """useShortName=True y todos los registros sin shortname → campaña abortada."""
    from modules.process.domain.models.process_dto import RulesCountry

    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(),
        storage=AnalysisStorage("sms_flow/shortname_missing_aborts"),
    )

    rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": True})
    payload = make_payload(content="Mensaje sin shortname.", rulesCountry=rules)
    df = await read_df(payload)

    with pytest.raises(ValueError, match="SHORTNAME_REQUIRED_IN_ALL"):
        await processor.process(df, payload)


@pytest.mark.anyio
async def test_sms_flow_shortname_partial_match_aborts_campaign():
    """Si algunos registros tienen el shortname y otros no → campaña abortada."""
    from modules.process.domain.models.process_dto import RulesCountry

    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(),
        storage=AnalysisStorage("sms_flow/shortname_partial"),
    )

    # data.csv tiene 2 filas: nombre="esteban xde" y nombre="otro".
    # El template referencia {nombre}; el shortname "esteban" solo aparece en una.
    rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": True})
    payload = make_payload(
        content="Hola {nombre}, este es tu mensaje.",
        shortname="esteban",
        rulesCountry=rules,
    )
    df = await read_df(payload)

    with pytest.raises(ValueError, match="SHORTNAME_REQUIRED_IN_ALL"):
        await processor.process(df, payload)


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
# Fix 1 — shortname condicional según useShortName
# ---------------------------------------------------------------------------

def test_sms_shortname_required_only_when_rule_active():
    """Shortname vacío es rechazado solo cuando useShortName=True."""
    from pydantic import ValidationError
    from modules.process.domain.models.process_dto import RulesCountry

    rules_on  = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": True})
    rules_off = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": False})

    content_with = "Hola SAEM3 este es el mensaje."

    for valor_invalido in ["", "   "]:
        base = make_payload(shortname=valor_invalido, content=content_with,
                            subService="informative", rulesCountry=rules_on).model_dump()
        with pytest.raises(ValidationError):
            SmsDataProcessingDTO.model_validate(base)

    base = make_payload(shortname="", content="Mensaje sin shortname.",
                        subService="informative", rulesCountry=rules_off).model_dump()
    SmsDataProcessingDTO.model_validate(base)


def test_sms_content_missing_shortname_accepted_at_dto():
    """El DTO ya NO rechaza content sin shortname (validación delegada al pipeline
    ShortNameRegulation, que valida sobre el mensaje real reemplazado para soportar
    shortnames que vienen dentro de {tag} del CSV)."""
    from modules.process.domain.models.process_dto import RulesCountry

    rules_on = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": True})
    base = make_payload(shortname="SAEM3", content="Mensaje sin el remitente.",
                        subService="informative", rulesCountry=rules_on).model_dump()
    # No debe lanzar ValidationError — el shortname puede llegar vía {tag} reemplazado.
    SmsDataProcessingDTO.model_validate(base)


def test_sms_subservice_invalid_rejected():
    """subService inválido es rechazado con ValidationError."""
    from pydantic import ValidationError

    for valor in ["unitario", "", "broadcast"]:
        base = make_payload(shortname="SAEM3").model_dump()
        base["subService"] = valor
        with pytest.raises(ValidationError):
            SmsDataProcessingDTO.model_validate(base)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_sms_flow_all_excluded_returns_success_false():
    """Cuando todos los registros son excluidos, success=False y el Parquet se guarda igual."""
    scenario = "sms_flow/all_excluded"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        # Rangos que no cubren ningún número del archivo → todos quedan sin operador
        numeration_service=numeration_mock(starts=[9000000000], ends=[9099999999], operators=["NONE"]),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    result = await processor.process(df, payload)
    save_summary(scenario, result)

    assert result["success"] is False
    assert result["error"]["code"] == "NO_VALID_RECORDS"
    assert result["summaryGeneral"]["total_records"] == 0
    assert result["summaryGeneral"]["total_excluded"] == 2

    # El Parquet se guarda aunque todos sean excluidos — la auditoría queda intacta
    assert len(storage.saved_dfs) == 1
    saved = storage.last_df()
    assert len(saved) == 2
    assert not saved[Cols.is_ok].any()
    assert (saved[Cols.error_code] == ExclusionReason.NO_OPERATOR).all()


@pytest.mark.anyio
async def test_sms_flow_zero_cost_produces_zero_credits():
    """Cost=0 produce credits=0 y unit_value=0 sin errores."""
    scenario = "sms_flow/zero_cost"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        numeration_service=numeration_mock(),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(costs=[("57", 0.0, "COLOMBIA")]),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    result = await processor.process(df, payload)
    save_summary(scenario, result)

    assert result["success"] is True
    assert result["summaryGeneral"]["total_credits"] == 0.0
    group = result["summaryGroup"][0]
    assert group["credits"] == 0.0
    assert group["unit_value"] == 0.0


@pytest.mark.anyio
async def test_sms_flow_unit_value_zero_when_group_all_excluded():
    """unit_value=0 cuando todos los registros de un operador están excluidos (no NaN)."""
    scenario = "sms_flow/unit_value_zero_excluded"
    storage = AnalysisStorage(scenario)
    processor = SmsProcessor(
        # Solo CLARO cubre el primer número; el segundo queda sin operador
        numeration_service=numeration_mock(
            starts=[3000000000], ends=[3009999999], operators=["CLARO"]
        ),
        exclusion_source=exclusion_mock(col="number"),
        cost_service=cost_mock(costs=[("57", 0.5, "COLOMBIA")]),
        storage=storage,
    )

    payload = make_payload()
    df = await read_df(payload)
    result = await processor.process(df, payload)

    for group in result["summaryGroup"]:
        uv = group["unit_value"]
        assert uv is not None, f"unit_value no debe ser None en grupo {group['operator']}"
        assert not (uv != uv), f"unit_value no debe ser NaN en grupo {group['operator']}"  # NaN check
