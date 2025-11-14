from fastapi import APIRouter, Depends
from typing import Literal
from modules.data_processing.application.schemas.preload_camp_schema import DataProcessingDTO
from modules._common.infrastructure.cache.redis import get_redis_client
from fastapi.responses import JSONResponse
from modules.data_processing.infrastructure.builders.factory import UseCaseFactory
from modules.data_processing.infrastructure.depends import get_databases

router = APIRouter()

@router.post("/data-processing/{service}")
async def preload_campaigns(
    service: Literal["sms", "email", "call_blasting", "api_call"],
    payload: DataProcessingDTO,
    cache: Depends = Depends(get_redis_client),
    dbs = Depends(get_databases),
):
    try:
        use_case = UseCaseFactory.create(service, payload, dbs, cache)
        return await use_case.execute(payload) 
    except FileNotFoundError as e:
        return JSONResponse(
            status_code=404,
            content={"message": str(e)}
        )
    except ValueError as e:
        return JSONResponse(
            status_code=400,
            content={"message": str(e)}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"message": "An unexpected error occurred: " + str(e)}
        )
    

@router.post("/processing/{service}")
async def process_data(
    service: Literal["sms", "email", "call_blasting", "api_call"],
    payload: DataProcessingDTO
):
    pass



