from fastapi import APIRouter, Depends
from ..schemas.preload_camp_schema import MainModel
from src.core.db.mysql_connect import get_mysql_db
from src.core.cache.redis_connect import get_redis_client
from src.core.logger.logger_adapter import LoggerAdapter
from src.core.db.mysql_adapter import SQLAlchemyAdapter
from src.core.cache.redis_adapter import RedisCache
from typing import Literal
from ..repositories.tarriff_cost import TariffRepository

router = APIRouter()

logger = LoggerAdapter("PreloadCampaigns")

@router.post("/preload_campaigns/{service}")
async def preload_campaigns(
    service: Literal["sms", "email", "call_blasting"],
    payload: MainModel,
    saem3_db: Depends = Depends(get_mysql_db),
    cache: Depends = Depends(get_redis_client)
):
    logger.info(f"Preloading campaigns for service: {service} with payload: {payload.content}")

    tariffRepo = TariffRepository(
        db=SQLAlchemyAdapter(saem3_db),
        cache=RedisCache(cache),
        logger=logger
    )

    result = tariffRepo.get_tariff(
        country_id=1,
        tariff_id=1,
        service=service
    )

    # Logic to preload campaigns goes here
    return {"campaign_id": 200, "result": result}