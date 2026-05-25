APP_TITLE = "API Data Process"

APP_VERSION = "2.0.0"

APP_DESCRIPTION = (
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
)

OPENAPI_TAGS = [
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
    {
        "name": "Files",
        "description": "Utilidades para inspeccionar archivos antes de procesarlos.",
    },
]
