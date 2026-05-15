import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from modules.process.domain.models.process_dto import DataProcessingDTO, SmsDataProcessingDTO
from modules.process.domain.models.confirm_dto import ConfirmRequest
from modules.process.domain.enums.services import ServiceType
from modules.process.infrastructure.deps import ProcessSharedDeps
from modules.process.app.process.factory import ProcessorFactory
from modules.process.app.use_case.process import ProcessDataUseCase
from modules.process.app.process.sms import SmsProcessor
from modules.process.app.process.callblasting import CallBlastingProcessor
from modules.process.app.process.email import EmailProcessor
from modules.process.app.services.numeration import NumerationService
from modules.process.app.services.cost import CostService
from modules.process.infrastructure.repositories.numeration import NumeracionRepository
from modules.process.infrastructure.repositories.cost import CostRepository
from modules._common.infrastructure.db import (
    get_db_portabilidad,
    get_db_saem3,
    get_db_telefonos_campanas,
    get_async_engine_campanas,
    get_sync_engine_email,
)
from modules.process.app.confirm.sms import SmsConfirmStrategy
from modules.process.app.confirm.email import EmailConfirmStrategy
from modules.process.app.confirm.call_blasting import CallBlastingConfirmStrategy
from modules.process.app.confirm.factory import ConfirmFactory
from modules.process.infrastructure.repositories.sms_confirm import SmsConfirmRepository
from modules.process.infrastructure.repositories.email_confirm import EmailConfirmRepository
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

router = APIRouter()

def get_shared_deps(request: Request) -> ProcessSharedDeps:
    return request.app.state.process_deps

def get_use_case(
    shared: ProcessSharedDeps = Depends(get_shared_deps),
    db_portabilidad=Depends(get_db_portabilidad),
    db_saem3=Depends(get_db_saem3),
) -> ProcessDataUseCase:
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

async def _parse_payload(service: ServiceType, request: Request) -> DataProcessingDTO:
    body = await request.json()
    try:
        if service == ServiceType.sms:
            return SmsDataProcessingDTO.model_validate(body)
        return DataProcessingDTO.model_validate(body)
    except ValidationError as exc:
        first = exc.errors()[0]
        detail = first["msg"].removeprefix("Value error, ")
        raise HTTPException(status_code=400, detail=detail)


@router.post(
    "/processing/{service}",
    summary="Procesar archivo de campaña",
    description=(
        "Procesa un archivo CSV o XLSX con registros de campaña para el servicio indicado.\n\n"
        "**Servicios disponibles:**\n"
        "- `sms` — valida números, asigna operador, calcula PDU y créditos. "
        "`subService`: `informative` | `landing`.\n"
        "- `email` — valida formato de correo, extrae dominio, calcula créditos. "
        "`subService`: `standard`.\n"
        "- `call_blasting` — valida números, calcula duración y créditos. "
        "`subService`: `standard` | `custom`.\n\n"
        "Retorna un resumen con totales por operador/dominio, créditos y registros excluidos. "
        "El archivo procesado se guarda como Parquet para ser usado en el confirm."
    ),
    responses={
        200: {"description": "Archivo procesado exitosamente. Retorna summaryGeneral, summaryGroup y violations."},
        400: {"description": "Payload inválido, subService no permitido, shortname requerido o archivo no encontrado."},
        500: {"description": "Error interno del servidor."},
    },
    tags=["Process Service"],
)
async def process_data(
    service: ServiceType,
    payload: DataProcessingDTO = Depends(_parse_payload),
    use_case: ProcessDataUseCase = Depends(get_use_case),
):
    try:
        result = await use_case(service, payload)
        return JSONResponse(status_code=200, content=result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error inesperado procesando servicio '%s'", service)
        raise HTTPException(status_code=500, detail="Error interno del servidor.")

def get_confirm_factory(
    shared: ProcessSharedDeps = Depends(get_shared_deps),
    db_telefonos_campanas=Depends(get_db_telefonos_campanas),
    async_engine_campanas: AsyncEngine = Depends(get_async_engine_campanas),
    sync_engine_email: Engine = Depends(get_sync_engine_email),
) -> ConfirmFactory:
    return ConfirmFactory({
        ServiceType.sms: SmsConfirmStrategy(
            SmsConfirmRepository(db_telefonos_campanas, async_engine_campanas), shared.storage
        ),
        ServiceType.email: EmailConfirmStrategy(
            EmailConfirmRepository(async_engine_email), shared.storage
        ),
        ServiceType.call_blasting: CallBlastingConfirmStrategy(shared.storage),
    })


@router.post(
    "/confirm/{service}",
    summary="Confirmar envío de campaña",
    description=(
        "Confirma el envío de una o más campañas para el servicio indicado. "
        "El `service` puede ser `sms`, `email` o `call_blasting`."
    ),
    responses={
        200: {"description": "Campañas confirmadas exitosamente."},
        400: {"description": "Servicio no soportado o datos inválidos."},
        500: {"description": "Error interno del servidor."},
    },
    tags=["Confirm Service"],
)
async def confirm_campaign(
    service: ServiceType,
    payload: ConfirmRequest,
    factory: ConfirmFactory = Depends(get_confirm_factory),
):
    """
    Confirma el envío de campañas para el servicio especificado.

    - **service**: tipo de servicio (`sms`, `email`, `call_blasting`)
    - **campaignId**: lista con los IDs de las campañas a confirmar
    """
    try:
        strategy = factory.get(service)
        result = await strategy.confirm(payload.campaignId, payload.codeGroup)
        return JSONResponse(status_code=200, content=result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error inesperado confirmando servicio '%s'", service)
        raise HTTPException(status_code=500, detail="Error interno del servidor.")


@router.get("/health")
async def health_check():
    return JSONResponse(status_code=200, content={"status": "ok"})
