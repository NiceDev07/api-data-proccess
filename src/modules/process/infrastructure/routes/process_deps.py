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
from modules.process.app.process.factory import ProcessorFactory
from modules.process.app.process.sms import SmsProcessor
from modules.process.app.services.cost import CostService
from modules.process.app.services.numeration import NumerationService
from modules.process.app.use_case.process import ProcessDataUseCase
from modules.process.domain.enums.services import ServiceType
from modules.process.domain.models.confirm_dto import ConfirmRequest
from modules.process.domain.models.process_dto import (
    CallBlastingDataProcessingDTO,
    DataProcessingDTO,
    SmsDataProcessingDTO,
)
from modules.process.infrastructure.startup.deps import ProcessSharedDeps
from modules.process.infrastructure.repositories.callblasting_confirm import CallBlastingConfirmRepository
from modules.process.infrastructure.repositories.cost import CostRepository
from modules.process.infrastructure.repositories.email_confirm import EmailConfirmRepository
from modules.process.infrastructure.repositories.numeration import NumeracionRepository
from modules.process.infrastructure.repositories.sms_confirm import SmsConfirmRepository


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
            numeration_service, shared.exclusion_source, cost_service, shared.storage,
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


def _format_validation_error(exc: ValidationError) -> str:
    # Extrae el primer error de Pydantic y lo formatea como "campo: mensaje"
    error = exc.errors()[0]
    msg = error["msg"].removeprefix("Value error, ")
    field = error.get("loc", ())[-1] if error.get("loc") else None
    return f"{field}: {msg}" if field else msg


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
