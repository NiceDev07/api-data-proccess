from fastapi import FastAPI
from modules.data_processing.infrastructure.routes.preload_camp import router as preload_router
from config.settings import settings

def create_app() -> FastAPI:
    app = FastAPI()
    app.include_router(preload_router, prefix=settings.PREFIX_APP, tags=["Data Processing Service"])
    return app

app = create_app()
