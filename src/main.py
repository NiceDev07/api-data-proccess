from fastapi import FastAPI
# from modules.data_processing.infrastructure.routes.preload_camp import router as preload_router
from modules.process.infrastructure.routes.process import router as process_router
from config.settings import settings
from lifespan import lifespan

def create_app() -> FastAPI:
    app = FastAPI(
        lifespan=lifespan
    )
    # app.include_router(preload_router, prefix=settings.PREFIX_APP, tags=["Data Processing Service"])
    app.include_router(process_router, prefix=settings.PREFIX_APP, tags=["Process Service"])
    return app

app = create_app()
