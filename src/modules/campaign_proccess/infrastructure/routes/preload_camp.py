from fastapi import APIRouter, Depends
from typing import Literal
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from core.db.mysql_connect import get_db_saem3
from core.cache.redis_connect import get_redis_client
from core.file.validators.local_file_validator import LocalFileValidator
from core.file.readers.csv_reader import CSVReader
from core.logger.logger_adapter import LoggerAdapter
from modules.campaign_proccess.application.use_cases.sms_proccess import SMSUseCase
from fastapi.responses import JSONResponse
router = APIRouter()

logger = LoggerAdapter("PreloadCampaigns")

@router.post("/preload_campaigns/{service}")
async def preload_campaigns(
    service: Literal["sms", "email", "call_blasting"],
    payload: PreloadCampDTO,
    saem3_db: Depends = Depends(get_db_saem3),
    cache: Depends = Depends(get_redis_client)
):
    
    use_case = SMSUseCase(
        file_validator=LocalFileValidator(),
        file_reader=CSVReader(payload.configFile.delimiter, payload.configFile.useHeaders)
    )
    try:
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



