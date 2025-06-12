from fastapi import FastAPI
from modules.campaign_proccess.infrastructure.routes.preload_camp import router as preload_router

app = FastAPI()
prefix = '/v2'

app.include_router(preload_router, prefix=prefix, tags=["preload_campaigns"])