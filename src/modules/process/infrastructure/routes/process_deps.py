"""
Dependencias de FastAPI para los endpoints de processing y confirm.

Separa la lógica de inyección de dependencias del router para mantener
process.py enfocado únicamente en las rutas y sus handlers.
"""
from fastapi import Depends, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine

from modules._common.infrastructure.db import (
    get_async_engine_callb,
    get_async_engine_campanas,
    get_async_engine_email,
    get_db_portabilidad,
    get_db_saem3,
    get_db_telefonos_campanas,
)
from modules.process.app.confirm.call_blasting import CallBlastingConfirmStrategy
from modules.process.app.confirm.email import EmailConfirmStrategy
from modules.process.app.confirm.factory import ConfirmFactory
from modules.process.app.confirm.sms import SmsConfirmStrategy
from modules.process.app.process.callblasting import CallBlastingProcessor
from modules.process.app.process.email import EmailProcessor
from modules.process.app.pipelines.sms.assign_operator import AssignOperator
from modules.process.app.pipelines.sms.assign_operator_routing import AssignOperatorRouting
from modules.process.app.process.factory import ProcessorFactory
from modules.process.app.process.sms import SmsProcessor
from modules.process.app.services.cost import CostService
from modules.process.app.services.numeration import NumerationService
from modules.process.app.use_case.process import ProcessDataUseCase
from modules.process.app.use_case.process_inline import ProcessSmsInlineUseCase
from modules.process.app.use_case.send_email_test import SendEmailTestUseCase
from modules.process.domain.enums.services import ServiceType
from modules.process.domain.models.confirm_dto import ConfirmRequest
from modules.process.domain.models.process_dto import (
    CallBlastingDataProcessingDTO,
    DataProcessingDTO,
    SmsDataProcessingDTO,
)
from modules.process.infrastructure.startup.deps import ProcessSharedDeps
from modules.process.infrastructure.repositories.confirm.callblasting import CallBlastingConfirmRepository
from modules.process.infrastructure.repositories.cost import CostRepository
from modules.process.infrastructure.repositories.confirm.email import EmailConfirmRepository
from modules.process.infrastructure.repositories.numeration import NumeracionRepository
from modules.process.infrastructure.repositories.portability import PortabilityRepository
from modules.process.infrastructure.repositories.confirm.sms import SmsConfirmRepository
from modules.process.infrastructure.email.smtp_sender import SmtpEmailSender
from modules.process.infrastructure.errors import build_error_detail
from config.settings import settings


# ── dependencias compartidas ───────────────────────────────────────────────────

def get_shared_deps(request: Request) -> ProcessSharedDeps:
    # Recupera las dependencias compartidas guardadas en el estado de la app al arrancar
    return request.app.state.process_deps


# ── use case ──────────────────────────────────────────────────────────────────

def get_use_case(
    shared: ProcessSharedDeps = Depends(get_shared_deps),
    db_portabilidad=Depends(get_db_portabilidad),
    db_saem3=Depends(get_db_saem3),
) -> ProcessDataUseCase:
    # Construye los servicios de numeración y costos con sus repositorios y cache
    numeration_service = NumerationService(NumeracionRepository(db_portabilidad), shared.cache)
    cost_service = CostService(CostRepository(db_saem3), shared.cache)

    processors = {
        ServiceType.sms: SmsProcessor(
            AssignOperator(numeration_service), shared.exclusion_source, cost_service, shared.storage,
        ),
        ServiceType.call_blasting: CallBlastingProcessor(
            numeration_service, shared.exclusion_source, cost_service,
            shared.storage, shared.duration_provider,
        ),
        ServiceType.email: EmailProcessor(
            shared.exclusion_source, cost_service, shared.storage,
        ),
    }

    return ProcessDataUseCase(
        file_reader_factory=shared.file_reader_factory,
        level_validator=shared.level_validator,
        processor_factory=ProcessorFactory(processors),
    )


# ── use case unitario (inline) ─────────────────────────────────────────────────

def get_inline_use_case(
    shared: ProcessSharedDeps = Depends(get_shared_deps),
    db_portabilidad=Depends(get_db_portabilidad),
    db_saem3=Depends(get_db_saem3),
) -> ProcessSmsInlineUseCase:
    # Mismo pipeline SMS que el masivo (validaciones, costo, PDU, créditos), pero el
    # paso de OPERADOR se resuelve con el SP consulta_operador_pais (operador + routing +
    # PORTABILIDAD por número) en vez de la numeración vectorizada. El unitario son
    # pocos números → un CALL por número es aceptable, y así el MSA solo persiste.
    cost_service = CostService(CostRepository(db_saem3), shared.cache)
    operator_step = AssignOperatorRouting(PortabilityRepository(db_portabilidad))
    processors = {
        ServiceType.sms: SmsProcessor(
            operator_step, shared.exclusion_source, cost_service, shared.storage,
        ),
    }
    return ProcessSmsInlineUseCase(
        processor_factory=ProcessorFactory(processors),
        level_validator=shared.level_validator,
    )


# ── send email test use case ──────────────────────────────────────────────────

def get_send_email_test_use_case(
    shared: ProcessSharedDeps = Depends(get_shared_deps),
) -> SendEmailTestUseCase:
    # Construye el sender SMTP con las credenciales del .env y lo inyecta junto
    # con la factory de readers que ya vive en las dependencias compartidas.
    sender = SmtpEmailSender(
        host     = settings.smtp_mail_test_host,
        port     = settings.smtp_mail_test_port,
        user     = settings.smtp_mail_test_domain_user,
        password = settings.smtp_mail_test_password,
        sender   = settings.smtp_mail_test_domain_send,
    )
    return SendEmailTestUseCase(
        sender              = sender,
        file_reader_factory = shared.file_reader_factory,
    )


# ── confirm factory ────────────────────────────────────────────────────────────

def get_confirm_factory(
    shared: ProcessSharedDeps = Depends(get_shared_deps),
    db_telefonos_campanas=Depends(get_db_telefonos_campanas),
    async_engine_campanas: AsyncEngine = Depends(get_async_engine_campanas),
    async_engine_email: AsyncEngine = Depends(get_async_engine_email),
    async_engine_callb: AsyncEngine = Depends(get_async_engine_callb),
) -> ConfirmFactory:
    # Registra una estrategia de confirm por cada servicio con su repositorio correspondiente
    return ConfirmFactory({
        ServiceType.sms: SmsConfirmStrategy(
            SmsConfirmRepository(db_telefonos_campanas, async_engine_campanas), shared.storage
        ),
        ServiceType.email: EmailConfirmStrategy(
            EmailConfirmRepository(async_engine_email), shared.storage
        ),
        ServiceType.call_blasting: CallBlastingConfirmStrategy(
            CallBlastingConfirmRepository(async_engine_callb), shared.storage
        ),
    })


# ── parsing y validación del payload ──────────────────────────────────────────

# SMS y Call Blasting usan DTOs extendidos con validaciones específicas de subService
_DTO_BY_SERVICE = {
    ServiceType.sms: SmsDataProcessingDTO,
    ServiceType.call_blasting: CallBlastingDataProcessingDTO,
}


def _format_validation_error(exc: ValidationError) -> dict:
    # Mapea los errores de Pydantic al formato {code, message} del proyecto.
    # - validators custom: usan formato "Value error, CODE: msg" → se extrae el CODE.
    # - campo required faltante: Pydantic da "Field required" sin código → MISSING_FIELD.
    # - otros (tipo incorrecto, etc.): INVALID_FIELD con el mensaje original.
    error = exc.errors()[0]
    msg = error["msg"]
    if msg.startswith("Value error, "):
        return build_error_detail(msg.removeprefix("Value error, "))
    field = error.get("loc", ())[-1] if error.get("loc") else "unknown"
    if error["type"] == "missing":
        return {"code": "MISSING_FIELD", "message": f"Field '{field}' is required."}
    return {"code": "INVALID_FIELD", "message": f"{field}: {msg}"}


async def parse_payload(service: ServiceType, request: Request) -> DataProcessingDTO:
    # Selecciona el DTO correcto según el servicio y valida el body de la petición
    body = await request.json()
    dto_class = _DTO_BY_SERVICE.get(service, DataProcessingDTO)
    try:
        return dto_class.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=_format_validation_error(exc))


async def parse_confirm_payload(request: Request) -> ConfirmRequest:
    # Valida el body del confirm y retorna un ConfirmRequest tipado
    body = await request.json()
    try:
        return ConfirmRequest.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=_format_validation_error(exc))
