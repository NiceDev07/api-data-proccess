from fastapi import FastAPI
# from modules.data_processing.infrastructure.routes.preload_camp import router as preload_router
from modules.process.infrastructure.routes.process import router as process_router
from config.settings import settings
from lifespan import lifespan

def create_app() -> FastAPI:
    app = FastAPI(
        title="API Data Process",
        description=(
            "Servicio de procesamiento de datos para campañas de comunicación masiva.\n\n"
            "Soporta tres servicios:\n"
            "- **SMS** (`sms`): procesamiento de archivos CSV/XLSX con números móviles.\n"
            "- **Email** (`email`): procesamiento de archivos con direcciones de correo.\n"
            "- **Call Blasting** (`call_blasting`): procesamiento de archivos para llamadas masivas.\n\n"
            "El flujo estándar es:\n"
            "1. `POST /v2/processing/{service}` — procesa el archivo y retorna el resumen.\n"
            "2. `POST /v2/confirm/{service}` — confirma e inserta los registros en la BD de campañas."
        ),
        version="2.0.0",
        lifespan=lifespan,
        openapi_tags=[
            {
                "name": "Process Service",
                "description": "Procesamiento de archivos CSV/XLSX para campañas de comunicación.",
            },
            {
                "name": "Confirm Service",
                "description": "Confirmación e inserción masiva de registros procesados en la BD de campañas.",
            },
        ],
    )
    # app.include_router(preload_router, prefix=settings.PREFIX_APP, tags=["Data Processing Service"])
    app.include_router(process_router, prefix=settings.PREFIX_APP, tags=["Process Service"])
    return app

app = create_app()
