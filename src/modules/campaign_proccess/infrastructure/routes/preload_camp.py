from fastapi import APIRouter, Depends
from typing import Literal
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from core.db.connection import get_db_saem3, get_db_forbidden_words
from core.cache.redis_connect import get_redis_client
from core.file.file_reader import FileReader
from modules.campaign_proccess.application.use_cases.sms_proccess import SMSUseCase
from fastapi.responses import JSONResponse
from modules.campaign_proccess.application.factories.use_case_factorie import UseCaseFactory

router = APIRouter()

@router.post("/preload_campaigns/{service}")
async def preload_campaigns(
    service: Literal["sms", "email", "call_blasting", "api_call"],
    payload: PreloadCampDTO,
    saem3_db: Depends = Depends(get_db_saem3),
    forbbiden_words: Depends = Depends(get_db_forbidden_words),
    cache: Depends = Depends(get_redis_client)
):
    try:
        dbs = {
            "saem3": saem3_db,
            "forbidden_words": forbbiden_words
        }
        use_case = UseCaseFactory.create(service, payload, dbs, cache)
        return use_case.execute(payload) 
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



