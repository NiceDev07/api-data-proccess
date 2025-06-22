from fastapi import APIRouter, Depends
from typing import Literal
from modules.data_processing.application.schemas.preload_camp_schema import PreloadCampDTO
from modules._common.infrastructure.db.saem3_db import get_db_saem3
from modules._common.infrastructure.cache.redis import get_redis_client
from modules._common.infrastructure.db.masivos_sms_db import get_db_masivos_sms
from fastapi.responses import JSONResponse
from modules.data_processing.infrastructure.builders.factory import UseCaseFactory

router = APIRouter()

@router.post("/preload_campaigns/{service}")
async def preload_campaigns(
    service: Literal["sms", "email", "call_blasting", "api_call"],
    payload: PreloadCampDTO,
    saem3_db: Depends = Depends(get_db_saem3),
    filter_db: Depends = Depends(get_db_masivos_sms),
    cache: Depends = Depends(get_redis_client)
):
    try:
        dbs = {
            "saem3": saem3_db,
            "filter_db": filter_db
        }
        use_case = UseCaseFactory.create(service, payload.configFile, dbs, cache)
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



