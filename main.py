from typing import Union
from fastapi import FastAPI
from config import settings
from src.modules.campaign_proccess.infrastructure.routes.preload_camp import router as preload_router

app = FastAPI()
prefix = '/v2'

app.include_router(preload_router, prefix=prefix, tags=["preload_campaigns"])