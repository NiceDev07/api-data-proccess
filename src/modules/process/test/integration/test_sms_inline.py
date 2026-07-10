"""
Flujo UNITARIO (inline): corre el pipeline SMS del masivo (validaciones, costo, PDU,
créditos) pero resuelve el OPERADOR + routing + portabilidad con el SP consulta_operador
(vía AssignOperatorRouting) en vez de la numeración vectorizada. Devuelve resultado por
número, listo para que el MSA solo persista.
"""
from unittest.mock import MagicMock

import pytest

from modules.process.app.pipelines.sms.assign_operator_routing import AssignOperatorRouting
from modules.process.app.process.factory import ProcessorFactory
from modules.process.app.process.sms import SmsProcessor
from modules.process.app.use_case.process_inline import ProcessSmsInlineUseCase
from modules.process.domain.enums.services import ServiceType
from modules.process.domain.models.inline_dto import InlineSmsRequest
from modules.process.domain.models.process_dto import InfoUserValidSend
from modules.process.infrastructure.validators.level_validator import LevelValidator

from modules.process.test.conftest import (
    BASE_RULES_SMS, cost_mock, exclusion_mock,
)

pytestmark = pytest.mark.anyio


class _PortabilityMock:
    """Emula el SP consulta_operador: CLARO (30x), MOVISTAR (32x), error (resto)."""

    async def consulta_operador(self, celular, code_flash, gcode, national_len, id_pais):
        national = str(celular)[-national_len:]
        if national.startswith("30"):
            return {"codigo": "1", "codigo_corto": "8901", "operador": "CLARO",
                    "tags": "t", "usuario_api": "claro_api", "servidor": "10.0.0.1",
                    "id_operador": 3}
        if national.startswith("32"):
            return {"codigo": "2", "codigo_corto": "8902", "operador": "MOVISTAR",
                    "tags": "t", "usuario_api": "movistar_api", "servidor": "10.0.0.2",
                    "id_operador": 5}
        return {"codigo": "ERROR", "codigo_corto": "99999", "operador": "ERROR",
                "tags": "ERROR", "usuario_api": "ERROR", "servidor": "ERROR",
                "idoperador": "ERROR"}


def _use_case() -> ProcessSmsInlineUseCase:
    processor = SmsProcessor(
        AssignOperatorRouting(_PortabilityMock()),   # operador vía SP (unitario)
        exclusion_mock(),
        cost_mock(),              # 0.5 por prefijo 57
        MagicMock(),              # storage: no se usa en el flujo inline
    )
    factory = ProcessorFactory({ServiceType.sms: processor})
    return ProcessSmsInlineUseCase(processor_factory=factory, level_validator=LevelValidator())


def _req(numbers, **over) -> InlineSmsRequest:
    base = dict(
        content="Hola mundo",
        tariffId=1,
        subService="informative",
        rulesCountry=BASE_RULES_SMS,
        infoUserValidSend=InfoUserValidSend(levelUser=2, demographic=""),
        numbers=numbers,
    )
    base.update(over)
    return InlineSmsRequest(**base)


async def test_inline_devuelve_resultado_por_numero():
    rows = await _use_case()(_req([3005973563, 3208392650]))
    assert len(rows) == 2
    by_op = {r["operator"] for r in rows}
    assert by_op == {"CLARO", "MOVISTAR"}
    for r in rows:
        assert r["isOk"] is True
        assert r["pdu"] >= 1
        assert r["credits"] >= 0
        # routing resuelto por el SP → el MSA solo persiste
        assert r["shortCode"] is not None
        assert r["userApi"] is not None
        assert r["server"] is not None
        assert r["operatorId"] > 0
        # number = prefijo país + nacional; national = nacional (para el envío)
        assert str(r["number"]).startswith("57")
        assert str(r["number"]).endswith(str(r["national"]))


async def test_inline_numero_sin_operador_se_marca_invalido():
    # El SP responde codigo_corto=99999 → NO_OPERATOR (fail-closed)
    rows = await _use_case()(_req([9999999999]))
    assert len(rows) == 1
    assert rows[0]["isOk"] is False
    assert rows[0]["errorCode"] == "NO_OPERATOR"
    assert rows[0]["shortCode"] is None
    assert rows[0]["operatorId"] == 0


async def test_inline_acepta_numero_con_prefijo():
    # Ya trae el 57 → CleanData lo reduce a nacional → el SP resuelve igual
    rows = await _use_case()(_req([573005973563]))
    assert len(rows) == 1
    assert rows[0]["isOk"] is True
    assert rows[0]["operator"] == "CLARO"
    assert rows[0]["shortCode"] == "8901"     # routing del SP (mock)
    assert rows[0]["operatorId"] == 3
    assert str(rows[0]["number"]) == "573005973563"


async def test_inline_operador_resuelto_sin_routing_completo_se_marca_invalido():
    # El SP a veces resuelve operador (codigo_corto válido, no-error) pero sin
    # usuario_api/servidor completos — antes esto pasaba como isOk=True con
    # routing null; ahora se invalida como NO_OPERATOR (fail-closed real).
    class _PartialRoutingMock:
        async def consulta_operador(self, celular, code_flash, gcode, national_len, id_pais):
            return {"codigo": "1", "codigo_corto": "8901", "operador": "CLARO",
                    "tags": "t", "usuario_api": None, "servidor": "10.0.0.1",
                    "id_operador": 3}

    processor = SmsProcessor(
        AssignOperatorRouting(_PartialRoutingMock()), exclusion_mock(), cost_mock(), MagicMock(),
    )
    factory = ProcessorFactory({ServiceType.sms: processor})
    use_case = ProcessSmsInlineUseCase(processor_factory=factory, level_validator=LevelValidator())

    rows = await use_case(_req([3005973563]))
    assert rows[0]["isOk"] is False
    assert rows[0]["errorCode"] == "NO_OPERATOR"
    assert rows[0]["shortCode"] is None


async def test_inline_flash_selecciona_code_flash_saem2():
    # useFlash=True → el paso llama al SP con code_flash='saem2'
    captured = {}

    class _Spy(_PortabilityMock):
        async def consulta_operador(self, celular, code_flash, gcode, national_len, id_pais):
            captured["flash"] = code_flash
            return await super().consulta_operador(celular, code_flash, gcode, national_len, id_pais)

    processor = SmsProcessor(
        AssignOperatorRouting(_Spy()), exclusion_mock(), cost_mock(), MagicMock(),
    )
    factory = ProcessorFactory({ServiceType.sms: processor})
    use_case = ProcessSmsInlineUseCase(processor_factory=factory, level_validator=LevelValidator())

    await use_case(_req([3005973563], useFlash=True))
    assert captured["flash"] == "saem2"
