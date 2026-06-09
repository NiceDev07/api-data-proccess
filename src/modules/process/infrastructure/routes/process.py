import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from modules.process.app.use_case.process import ProcessDataUseCase
from modules.process.app.confirm.factory import ConfirmFactory
from modules.process.domain.enums.services import ServiceType
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.models.confirm_dto import ConfirmRequest
from modules.process.infrastructure.routes.process_deps import (
    get_use_case,
    get_confirm_factory,
    parse_payload,
    parse_confirm_payload,
)
from modules.process.infrastructure.routes.docs import (
    PROCESSING_BODY_SCHEMA, PROCESSING_EXAMPLES, PROCESSING_DESCRIPTION, PROCESSING_RESPONSES,
    CONFIRM_BODY_SCHEMA, CONFIRM_EXAMPLES, CONFIRM_DESCRIPTION, CONFIRM_RESPONSES,
)
from modules.process.infrastructure.errors import build_error_detail

logger = logging.getLogger(__name__)

router = APIRouter()


# ── rutas ──────────────────────────────────────────────────────────────────────

@router.post(
    "/processing/{service}",
    summary="Procesar archivo de campaña",
    description=PROCESSING_DESCRIPTION,
    responses=PROCESSING_RESPONSES,
    tags=["Processing"],
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": PROCESSING_BODY_SCHEMA,
                    "examples": PROCESSING_EXAMPLES,
                }
            },
        }
    },
)
async def process_data(
    service: ServiceType,
    payload: DataProcessingDTO = Depends(parse_payload),
    use_case: ProcessDataUseCase = Depends(get_use_case),
):
    try:
        result = await use_case(service, payload)
        return JSONResponse(status_code=200, content=result)
    except FileNotFoundError as e:
        logger.warning("Archivo no encontrado en processing [%s] | path=%s | error=%s",
                       service, payload.configFile.folder, e)
        raise HTTPException(status_code=404, detail=build_error_detail(str(e)))
    except ValueError as e:
        logger.warning("Error de validación en processing [%s] | path=%s | error=%s",
                       service, payload.configFile.folder, e)
        raise HTTPException(status_code=400, detail=build_error_detail(str(e)))
    except Exception:
        logger.exception("Error inesperado procesando servicio '%s'", service)
        raise HTTPException(status_code=500, detail=build_error_detail("INTERNAL_SERVER_ERROR: Error interno del servidor."))


@router.post(
    "/confirm/{service}",
    summary="Confirmar e insertar registros de campaña",
    description=CONFIRM_DESCRIPTION,
    responses=CONFIRM_RESPONSES,
    tags=["Confirm"],
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": CONFIRM_BODY_SCHEMA,
                    "examples": CONFIRM_EXAMPLES,
                }
            },
        }
    },
)
async def confirm_campaign(
    service: ServiceType,
    payload: ConfirmRequest = Depends(parse_confirm_payload),
    factory: ConfirmFactory = Depends(get_confirm_factory),
):
    try:
        strategy = factory.get(service)
        result = await strategy.confirm(payload.campaignId, payload.codeGroup)
        return JSONResponse(status_code=200, content=result)
    except FileNotFoundError as e:
        logger.warning("Archivo no encontrado en confirm [%s] | campaigns=%s | error=%s",
                       service, payload.campaignId, e)
        raise HTTPException(status_code=404, detail=build_error_detail(str(e)))
    except ValueError as e:
        logger.warning("Error de validación en confirm [%s] | campaigns=%s | error=%s",
                       service, payload.campaignId, e)
        raise HTTPException(status_code=400, detail=build_error_detail(str(e)))
    except Exception:
        logger.exception("Error inesperado confirmando servicio '%s'", service)
        raise HTTPException(status_code=500, detail=build_error_detail("INTERNAL_SERVER_ERROR: Error interno del servidor."))


@router.get(
    "/health",
    summary="Health check",
    description="Verifica que el servicio esté activo y respondiendo.",
    responses={200: {"description": "Servicio operativo."}},
    tags=["Health"],
)
async def health_check():
    return JSONResponse(status_code=200, content={"status": "ok"})
