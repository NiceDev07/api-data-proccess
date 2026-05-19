from fastapi import FastAPI
# from modules.data_processing.infrastructure.routes.preload_camp import router as preload_router
from modules.process.infrastructure.routes.process import router as process_router
from config.settings import settings
from lifespan import lifespan

def create_app() -> FastAPI:
    app = FastAPI(
        title="API Data Process",
        description=(
            "Servicio de procesamiento masivo de datos para campañas de comunicación (SMS, Email y Call Blasting).\n\n"
            "## Flujo de uso\n"
            "1. **POST /v2/processing/{service}** — Sube el archivo CSV o XLSX, valida los registros, "
            "calcula créditos y deja el resultado listo como Parquet.\n"
            "2. **POST /v2/confirm/{service}** — Toma ese Parquet e inserta los registros válidos "
            "en la base de datos de campañas.\n\n"
            "## Servicios disponibles\n"
            "| Servicio | Valor en la URL | Sub-servicios |\n"
            "|---|---|---|\n"
            "| SMS | sms | informative, landing |\n"
            "| Email | email | standard |\n"
            "| Call Blasting | call_blasting | standard, custom |\n\n"
            "## Cosas a tener en cuenta\n"
            "- **configFile.folder** debe ser la ruta completa al archivo, no al directorio que lo contiene.\n"
            "- Los registros excluidos aparecen en violations (SMS) o en summaryGroup con total_excluded.\n"
            "- Para Call Blasting standard hay que enviar audioDuration en segundos o audioPath con la ruta al audio."
        ),
        version="2.0.0",
        lifespan=lifespan,
        openapi_tags=[
            {
                "name": "Processing",
                "description": (
                    "Lee y valida el archivo CSV o XLSX, calcula créditos por operador o dominio "
                    "y guarda el resultado como Parquet. Siempre hay que correr este endpoint antes del confirm."
                ),
            },
            {
                "name": "Confirm",
                "description": (
                    "Inserta los registros válidos del Parquet en la base de datos de campañas. "
                    "Requiere haber ejecutado processing primero con el mismo campaignId o codeGroup."
                ),
            },
            {
                "name": "Health",
                "description": "Verifica que el servicio esté corriendo.",
            },
        ],
    )
    # app.include_router(preload_router, prefix=settings.PREFIX_APP, tags=["Data Processing Service"])
    app.include_router(process_router, prefix=settings.PREFIX_APP)
    return app

app = create_app()
