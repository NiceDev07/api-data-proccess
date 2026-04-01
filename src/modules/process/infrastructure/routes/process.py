import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.enums.services import ServiceType
from modules.process.infrastructure.deps import ProcessSharedDeps

logger = logging.getLogger(__name__)

from modules.process.app.process.factory import ProcessorFactory
from modules.process.app.use_case.process import ProcessDataUseCase
from modules.process.app.process.sms import SmsProcessor
from modules.process.app.process.callblasting import CallBlastingProcessor
from modules.process.app.process.email import EmailProcessor
from modules.process.app.services.numeration import NumerationService
from modules.process.app.services.cost import CostService
from modules.process.infrastructure.repositories.numeration import NumeracionRepository
from modules.process.infrastructure.repositories.cost import CostRepository
from modules._common.infrastructure.db import get_db_portabilidad, get_db_saem3

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


@router.post("/processing/{service}")
async def process_data(
    service: ServiceType,
    payload: DataProcessingDTO,
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


@router.get("/health")
async def health_check():
    return JSONResponse(status_code=200, content={"status": "ok"})
