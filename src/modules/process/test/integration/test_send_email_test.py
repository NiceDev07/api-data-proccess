"""
Tests de integración del SendEmailTestUseCase.

- El sender SMTP se mockea con AsyncMock para no hacer envíos reales.
- El reader usa CSVs temporales escritos en disco para ejercer el flujo real.
"""
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from modules.process.app.files.factory import ReaderFileFactory
from modules.process.app.use_case.send_email_test import SendEmailTestUseCase
from modules.process.domain.models.process_dto import (
    ConfigFile,
    EmailTestRequest,
    EmailRecipient,
)

pytestmark = pytest.mark.anyio


# ── helpers ───────────────────────────────────────────────────────────────────

def _write_csv(folder: Path, name: str, content: str) -> Path:
    path = folder / name
    path.write_text(content, encoding="utf-8")
    return path


def _make_payload(
    folder: Path,
    file_name: str,
    recipients: list[EmailRecipient],
    content: str = "Hola {nombre}, tu monto es {monto}",
    subject: str = "Asunto {nombre}",
) -> EmailTestRequest:
    return EmailTestRequest(
        subService="massive",
        content=content,
        subject=subject,
        listDemographics=recipients,
        configFile=ConfigFile(
            folder=str(folder / file_name),
            file=file_name,
            delimiter=",",
            useHeaders=True,
            nameColumnDemographic="correo",
            userIdentifier=False,
            nameColumnIdentifier="",
            fileRecords=100,
        ),
    )


def _make_use_case(sender_mock: AsyncMock) -> SendEmailTestUseCase:
    return SendEmailTestUseCase(
        sender=sender_mock,
        file_reader_factory=ReaderFileFactory(),
    )


# ── tests ─────────────────────────────────────────────────────────────────────

async def test_happy_path_envia_a_todos_los_destinatarios_validos(tmp_path):
    """3 destinatarios válidos: cada uno recibe content/subject personalizado."""
    _write_csv(tmp_path, "data.csv",
               "nombre,monto,correo\n"
               "Juan,100,a@m.com\n"
               "Maria,200,b@m.com\n"
               "Carlos,300,c@m.com\n")

    sender = AsyncMock()
    sender.send = AsyncMock(return_value=True)

    recipients = [
        EmailRecipient(demographic="dest1@mail.com", status=True),
        EmailRecipient(demographic="dest2@mail.com", status=True),
        EmailRecipient(demographic="dest3@mail.com", status=True),
    ]
    payload = _make_payload(tmp_path, "data.csv", recipients)

    result = await _make_use_case(sender)(payload)

    assert len(result) == 3
    assert all(r["valid"] and r["valid_send"] for r in result)
    # Cada destinatario tiene su fila correspondiente del CSV
    assert "Juan" in result[0]["content"]   and "100" in result[0]["content"]
    assert "Maria" in result[1]["content"]  and "200" in result[1]["content"]
    assert "Carlos" in result[2]["content"] and "300" in result[2]["content"]
    assert sender.send.await_count == 3


async def test_destinatarios_invalidos_se_reportan_pero_no_se_envian(tmp_path):
    """2 válidos + 2 inválidos (status=false / email mal formado): solo se llama send para los válidos."""
    _write_csv(tmp_path, "data.csv", "nombre,correo\nJuan,a@m.com\nMaria,b@m.com\n")

    sender = AsyncMock()
    sender.send = AsyncMock(return_value=True)

    recipients = [
        EmailRecipient(demographic="ok1@mail.com",  status=True),
        EmailRecipient(demographic="no-es-email",   status=True),    # email inválido
        EmailRecipient(demographic="off@mail.com",  status=False),   # status false
        EmailRecipient(demographic="ok2@mail.com",  status=True),
    ]
    payload = _make_payload(tmp_path, "data.csv", recipients)

    result = await _make_use_case(sender)(payload)

    assert len(result) == 4
    assert [r["valid"] for r in result] == [True, False, False, True]
    assert [r["valid_send"] for r in result] == [True, False, False, True]
    assert sender.send.await_count == 2


async def test_todos_invalidos_lanza_no_valid_recipients(tmp_path):
    """Si ningún destinatario pasa la validación, lanza ValueError con NO_VALID_RECIPIENTS."""
    _write_csv(tmp_path, "data.csv", "nombre,correo\nJuan,a@m.com\n")

    sender = AsyncMock()
    sender.send = AsyncMock(return_value=True)

    recipients = [
        EmailRecipient(demographic="no-es-email",  status=True),
        EmailRecipient(demographic="off@mail.com", status=False),
    ]
    payload = _make_payload(tmp_path, "data.csv", recipients)

    with pytest.raises(ValueError, match="NO_VALID_RECIPIENTS"):
        await _make_use_case(sender)(payload)

    assert sender.send.await_count == 0


async def test_mas_destinatarios_que_filas_reusa_ultima_fila(tmp_path):
    """5 destinatarios válidos pero el CSV solo tiene 2 filas → los últimos 3 usan la fila 2."""
    _write_csv(tmp_path, "data.csv", "nombre,correo\nAna,a@m.com\nLuis,b@m.com\n")

    sender = AsyncMock()
    sender.send = AsyncMock(return_value=True)

    recipients = [EmailRecipient(demographic=f"d{i}@m.com", status=True) for i in range(5)]
    payload = _make_payload(tmp_path, "data.csv", recipients, content="Hola {nombre}")

    result = await _make_use_case(sender)(payload)

    assert len(result) == 5
    assert result[0]["content"] == "Hola Ana"
    assert result[1]["content"] == "Hola Luis"
    # Los últimos 3 reutilizan la última fila (Luis)
    assert result[2]["content"] == "Hola Luis"
    assert result[3]["content"] == "Hola Luis"
    assert result[4]["content"] == "Hola Luis"


async def test_smtp_falla_para_un_destinatario_pero_sigue(tmp_path):
    """Si el sender devuelve False para un envío, valid_send=False pero continúa con el resto."""
    _write_csv(tmp_path, "data.csv", "nombre,correo\nA,a@m.com\nB,b@m.com\nC,c@m.com\n")

    sender = AsyncMock()
    # El segundo envío falla, los otros dos pasan
    sender.send = AsyncMock(side_effect=[True, False, True])

    recipients = [EmailRecipient(demographic=f"d{i}@m.com", status=True) for i in range(3)]
    payload = _make_payload(tmp_path, "data.csv", recipients)

    result = await _make_use_case(sender)(payload)

    assert [r["valid_send"] for r in result] == [True, False, True]
    assert sender.send.await_count == 3


async def test_tag_faltante_en_csv_queda_tal_cual(tmp_path):
    """Si el template tiene {desconocido} y la fila del CSV no lo trae, el tag se conserva."""
    _write_csv(tmp_path, "data.csv", "nombre,correo\nJuan,a@m.com\n")

    sender = AsyncMock()
    sender.send = AsyncMock(return_value=True)

    payload = _make_payload(
        tmp_path, "data.csv",
        [EmailRecipient(demographic="dest@m.com", status=True)],
        content="Hola {nombre}, código: {desconocido}",
    )
    result = await _make_use_case(sender)(payload)

    assert result[0]["content"] == "Hola Juan, código: {desconocido}"
