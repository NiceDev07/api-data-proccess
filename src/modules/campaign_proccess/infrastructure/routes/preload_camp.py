from fastapi import APIRouter, Depends
from typing import Literal
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from core.db.mysql_connect import get_mysql_db
from core.cache.redis_connect import get_redis_client
from core.logger.logger_adapter import LoggerAdapter

router = APIRouter()

logger = LoggerAdapter("PreloadCampaigns")

@router.post("/preload_campaigns/{service}")
async def preload_campaigns(
    service: Literal["sms", "email", "call_blasting"],
    payload: PreloadCampDTO,
    saem3_db: Depends = Depends(get_mysql_db),
    cache: Depends = Depends(get_redis_client)
):

    return {"campaign_id": 200}


