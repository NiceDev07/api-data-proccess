import copy
import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from modules.process.domain.models.process_dto import DataProcessingDTO, SmsDataProcessingDTO, CallBlastingDataProcessingDTO
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
    get_async_engine_email,
    get_async_engine_callb,
)
from modules.process.app.confirm.sms import SmsConfirmStrategy
from modules.process.app.confirm.email import EmailConfirmStrategy
from modules.process.app.confirm.call_blasting import CallBlastingConfirmStrategy
from modules.process.app.confirm.factory import ConfirmFactory
from modules.process.infrastructure.repositories.sms_confirm import SmsConfirmRepository
from modules.process.infrastructure.repositories.email_confirm import EmailConfirmRepository
from modules.process.infrastructure.repositories.callblasting_confirm import CallBlastingConfirmRepository
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

def _inline_schema_refs(schema: dict) -> dict:
    schema = copy.deepcopy(schema)
    defs = schema.pop("$defs", {})

    def resolve(obj):
        if isinstance(obj, dict):
            if "$ref" in obj:
                ref_name = obj["$ref"].split("/")[-1]
                if ref_name in defs:
                    return resolve(copy.deepcopy(defs[ref_name]))
            return {k: resolve(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [resolve(item) for item in obj]
        return obj

    return resolve(schema)


_PROCESSING_BODY_SCHEMA = _inline_schema_refs(DataProcessingDTO.model_json_schema())
_CONFIRM_BODY_SCHEMA = _inline_schema_refs(ConfirmRequest.model_json_schema())

_CONFIRM_EXAMPLES = {
    "sms": {
        "summary": "SMS",
        "value": {
            "campaignId": [229960],
            "codeGroup": "KXQM7291",
        },
    },
    "email": {
        "summary": "Email",
        "value": {
            "campaignId": [229961],
            "codeGroup": "BRTV4508",
        },
    },
    "call_blasting": {
        "summary": "Call Blasting",
        "value": {
            "campaignId": [229962],
            "codeGroup": "LPWZ6134",
        },
    },
}

_PROCESSING_EXAMPLES = {
    "sms_informativo": {
        "summary": "SMS — informativo",
        "value": {
            "content": "Estimado {nombre}, tiene una notificación pendiente.",
            "tariffId": 1,
            "campaignId": [999001],
            "codeGroup": "KXQM7291",
            "subService": "informative",
            "useExclusionList": False,
            "configListExclusion": None,
            "configFile": {
                "folder": "/ruta/al/archivo/campana.csv",
                "file": "campana.csv",
                "delimiter": ";",
                "useHeaders": True,
                "nameColumnDemographic": "telefono",
                "userIdentifier": False,
                "nameColumnIdentifier": "",
                "fileRecords": 5000,
            },
            "rulesCountry": {
                "idCountry": 1, "codeCountry": 57,
                "useCharacterSpecial": False,
                "limitCharacter": 160, "limitCharacterSpecial": 70,
                "numberDigitsMobile": 10, "numberDigitsFixed": 7,
                "useShortName": False,
            },
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        },
    },
    "email_standard": {
        "summary": "Email — standard",
        "value": {
            "content": "Estimado {nombre}, su solicitud ha sido procesada.",
            "subject": "Notificación de su cuenta",
            "tariffId": 1,
            "campaignId": [999002],
            "codeGroup": "BRTV4508",
            "subService": "standard",
            "useExclusionList": False,
            "configListExclusion": None,
            "configFile": {
                "folder": "/ruta/al/archivo/emails.xlsx",
                "file": "emails.xlsx",
                "delimiter": ";",
                "useHeaders": True,
                "nameColumnDemographic": "email",
                "userIdentifier": False,
                "nameColumnIdentifier": "",
                "fileRecords": 3000,
            },
            "rulesCountry": {
                "idCountry": 1, "codeCountry": 57,
                "useCharacterSpecial": False,
                "limitCharacter": 160, "limitCharacterSpecial": 70,
                "numberDigitsMobile": 10, "numberDigitsFixed": 7,
                "useShortName": False,
            },
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        },
    },
    "call_blasting_standard": {
        "summary": "Call Blasting — standard (audioDuration)",
        "value": {
            "content": "Estimado {nombre}, tiene una notificación pendiente en su cuenta.",
            "tariffId": 1,
            "campaignId": [999003],
            "codeGroup": "LPWZ6134",
            "subService": "standard",
            "audioDuration": 20,
            "audioPath": None,
            "useExclusionList": False,
            "configListExclusion": None,
            "configFile": {
                "folder": "/ruta/al/archivo/campana.xlsx",
                "file": "campana.xlsx",
                "delimiter": ";",
                "useHeaders": True,
                "nameColumnDemographic": "telefono",
                "userIdentifier": False,
                "nameColumnIdentifier": "",
                "fileRecords": 10000,
            },
            "rulesCountry": {
                "idCountry": 1, "codeCountry": 57,
                "useCharacterSpecial": False,
                "limitCharacter": 160, "limitCharacterSpecial": 70,
                "numberDigitsMobile": 10, "numberDigitsFixed": 7,
                "useShortName": False,
            },
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        },
    },
    "call_blasting_custom": {
        "summary": "Call Blasting — custom (mensaje personalizado por registro)",
        "value": {
            "content": "Estimado {nombre}, su saldo es {saldo} con fecha {fecha}.",
            "tariffId": 1,
            "campaignId": [999004],
            "codeGroup": "NFHJ8823",
            "subService": "custom",
            "audioDuration": None,
            "audioPath": None,
            "useExclusionList": False,
            "configListExclusion": None,
            "configFile": {
                "folder": "/ruta/al/archivo/campana_custom.xlsx",
                "file": "campana_custom.xlsx",
                "delimiter": ";",
                "useHeaders": True,
                "nameColumnDemographic": "telefono",
                "userIdentifier": False,
                "nameColumnIdentifier": "",
                "fileRecords": 10000,
            },
            "rulesCountry": {
                "idCountry": 1, "codeCountry": 57,
                "useCharacterSpecial": False,
                "limitCharacter": 160, "limitCharacterSpecial": 70,
                "numberDigitsMobile": 10, "numberDigitsFixed": 7,
                "useShortName": False,
            },
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        },
    },
}


def _format_validation_error(exc: ValidationError) -> str:
    error = exc.errors()[0]
    msg = error["msg"].removeprefix("Value error, ")
    field = error.get("loc", ())[-1] if error.get("loc") else None
    return f"{field}: {msg}" if field else msg


_DTO_BY_SERVICE = {
    ServiceType.sms: SmsDataProcessingDTO,
    ServiceType.call_blasting: CallBlastingDataProcessingDTO,
}

async def _parse_payload(service: ServiceType, request: Request) -> DataProcessingDTO:
    body = await request.json()
    dto_class = _DTO_BY_SERVICE.get(service, DataProcessingDTO)
    try:
        return dto_class.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=_format_validation_error(exc))


async def _parse_confirm_payload(request: Request) -> ConfirmRequest:
    body = await request.json()
    try:
        return ConfirmRequest.model_validate(body)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=_format_validation_error(exc))


@router.post(
    "/processing/{service}",
    summary="Procesar archivo de campaña",
    description=(
        "Lee el archivo CSV o XLSX indicado en configFile, valida cada registro según las reglas del servicio "
        "y guarda el resultado como Parquet. Ese Parquet es el que consume el endpoint de confirm.\n\n"
        "### Por servicio\n\n"
        "**SMS**\n"
        "- Valida longitud del número y asigna operador por rangos de numeración.\n"
        "- subService: informative o landing.\n"
        "- Si rulesCountry.useShortName es true, el campo shortname es obligatorio "
        "y debe estar incluido en el contenido del mensaje.\n\n"
        "**Email**\n"
        "- Valida el formato del correo y agrupa el resumen por dominio.\n"
        "- subService: standard.\n"
        "- El campo subject es obligatorio.\n\n"
        "**Call Blasting**\n"
        "- Valida el número, asigna operador y calcula duración y créditos del audio.\n"
        "- subService: standard o custom.\n"
        "- Para standard hay que enviar audioDuration en segundos o audioPath con la ruta al archivo de audio.\n"
        "- Para custom la duración se calcula desde el texto del mensaje, no se necesita audio previo.\n"
        "- Los números sin tarifa configurada quedan excluidos con razón NO_COST.\n\n"
        "### Notas\n"
        "- configFile.folder debe ser la ruta completa al archivo, no al directorio.\n"
        "- Usuario nivel 1: máximo 10 registros. Nivel 2 o superior: hasta 700 000.\n"
        "- El Parquet se identifica por campaignId o codeGroup. Ese mismo valor hay que enviarlo al confirm."
    ),
    responses={
        200: {"description": "Procesamiento exitoso. Retorna summaryGeneral, summaryGroup y violations."},
        400: {"description": "Payload inválido: campo faltante, subService no permitido, shortname requerido, o audioDuration/audioPath faltante para call_blasting standard."},
        404: {"description": "Archivo de campaña no encontrado en la ruta indicada."},
        500: {"description": "Error interno del servidor."},
    },
    tags=["Processing"],
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": _PROCESSING_BODY_SCHEMA,
                    "examples": _PROCESSING_EXAMPLES,
                }
            },
        }
    },
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
        logger.warning("Archivo no encontrado en processing [%s]: %s", service, e)
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        logger.warning("Error de validación en processing [%s]: %s", service, e)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error inesperado procesando servicio '%s'", service)
        raise HTTPException(status_code=500, detail="Error interno del servidor.")

def get_confirm_factory(
    shared: ProcessSharedDeps = Depends(get_shared_deps),
    db_telefonos_campanas=Depends(get_db_telefonos_campanas),
    async_engine_campanas: AsyncEngine = Depends(get_async_engine_campanas),
    async_engine_email: AsyncEngine = Depends(get_async_engine_email),
    async_engine_callb: AsyncEngine = Depends(get_async_engine_callb),
) -> ConfirmFactory:
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


@router.post(
    "/confirm/{service}",
    summary="Confirmar e insertar registros de campaña",
    description=(
        "Toma el Parquet que dejó el endpoint de processing e inserta los registros válidos "
        "en la base de datos de campañas.\n\n"
        "### Antes de llamar este endpoint\n"
        "Hay que haber corrido processing con el mismo campaignId o codeGroup. "
        "Si el Parquet no existe retorna 404.\n\n"
        "### Por servicio\n\n"
        "**SMS** — Inserta en telefonos_campanas usando LOAD DATA LOCAL INFILE por lotes.\n\n"
        "**Email** — Crea la tabla mail_{campaignId} si no existe e inserta con INSERT IGNORE.\n\n"
        "**Call Blasting** — Crea la tabla en PostgreSQL e inserta con ON CONFLICT DO NOTHING.\n\n"
        "### Payload requerido\n"
        "- **codeGroup**: el mismo valor enviado en processing. Se usa para localizar el Parquet.\n"
        "- **campaignId**: lista con al menos un ID de campaña. Se usa para nombrar las tablas e insertar registros.\n\n"
        "### Cómo se busca el archivo\n"
        "Se busca el Parquet por codeGroup. Si no existe, se busca por los IDs de campaña concatenados."
    ),
    responses={
        200: {"description": "Registros insertados. Retorna inserted con el total de filas confirmadas."},
        404: {"description": "Archivo Parquet no encontrado. Ejecute primero el endpoint de processing."},
        400: {"description": "Datos inválidos en el payload."},
        500: {"description": "Error interno del servidor."},
    },
    tags=["Confirm"],
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": _CONFIRM_BODY_SCHEMA,
                    "examples": _CONFIRM_EXAMPLES,
                }
            },
        }
    },
)
async def confirm_campaign(
    service: ServiceType,
    payload: ConfirmRequest = Depends(_parse_confirm_payload),
    factory: ConfirmFactory = Depends(get_confirm_factory),
):
    try:
        strategy = factory.get(service)
        result = await strategy.confirm(payload.campaignId, payload.codeGroup)
        return JSONResponse(status_code=200, content=result)
    except FileNotFoundError as e:
        logger.warning("Archivo no encontrado en confirm [%s]: %s", service, e)
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        logger.warning("Error de validación en confirm [%s]: %s", service, e)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Error inesperado confirmando servicio '%s'", service)
        raise HTTPException(status_code=500, detail="Error interno del servidor.")


@router.get(
    "/health",
    summary="Health check",
    description="Verifica que el servicio esté activo y respondiendo.",
    responses={200: {"description": "Servicio operativo."}},
    tags=["Health"],
)
async def health_check():
    return JSONResponse(status_code=200, content={"status": "ok"})
