from typing import Literal
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from modules.data_processing.application.schemas.preload_camp_schema import DataProcessingDTO
from modules.process.domain.enums.services import ServiceType 
from modules.process.infrastructure.process.factory import ProcessorFactory
from modules._common.infrastructure.cache.redis import get_redis_client
from modules.process.app.use_case.process import ProcessDataUseCase

router = APIRouter()

def get_factory(cache=Depends(get_redis_client)):
    return ProcessorFactory(cache)

@router.get("/health")
async def health_check():
    return JSONResponse(status_code=200, content={"status": "ok"})


@router.post("/processing/{service}")
async def process_data(
    service: ServiceType,
    payload: DataProcessingDTO,
    factory: ProcessorFactory = Depends(get_factory)
):
    try:
        processor = factory.create(service)                 # infra
        result = await ProcessDataUseCase(processor)(payload)   # application usa domain
        return JSONResponse(status_code=200, content=result)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="An unexpected error occurred: " + str(e))
