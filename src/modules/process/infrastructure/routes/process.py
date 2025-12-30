from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from modules.data_processing.application.schemas.preload_camp_schema import DataProcessingDTO
from modules.process.domain.enums.services import ServiceType 
from modules.process.app.process.factory import ProcessorFactory
from modules.process.app.use_case.process import ProcessDataUseCase
from modules.process.app.files.factory import ReaderFileFactory
from modules.process.infrastructure.validators.level_validator import LevelValidator
from modules.process.domain.enums.services import ServiceType
from modules.process.app.process.sms import SmsProcessor
from modules.process.app.services.numeration import NumerationService
from modules.process.infrastructure.repositories.numeration import NumeracionRepository
from modules._common.infrastructure.cache.redis import RedisCache, get_redis_client
from modules._common.infrastructure.db import get_db_portabilidad
from modules.process.infrastructure.exclusions.customer_exclusion_source import CustomerExclusionSource

router = APIRouter()

def get_process_data_use_case(
    redis_client=Depends(get_redis_client),
    db_portabilidad=Depends(get_db_portabilidad)
) -> ProcessDataUseCase:
    file_reader_factory = ReaderFileFactory()
    level_validator = LevelValidator(max_records=10)


    numeration_service = NumerationService(
        NumeracionRepository(db_portabilidad),
        RedisCache(redis_client)
    )

    exclusion_source = CustomerExclusionSource(file_reader_factory)

    processors = {
        ServiceType.sms: SmsProcessor(
                            numeration_service,
                            exclusion_source
                        ),
        # ServiceType.call_blasting: CallBlastingProcess(),
    }

    processor_factory = ProcessorFactory(processors)


    return ProcessDataUseCase(
        file_reader_factory=file_reader_factory,
        level_validator=level_validator,
        processor_factory=processor_factory
    )


@router.get("/health")
async def health_check():
    return JSONResponse(status_code=200, content={"status": "ok"})


@router.post("/processing/{service}")
async def process_data(
    service: ServiceType,
    payload: DataProcessingDTO,
    use_case: ProcessDataUseCase = Depends(get_process_data_use_case)
):
    try:
        result = await use_case(service, payload)
        return JSONResponse(status_code=200, content=result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="An unexpected error occurred: " + str(e))
