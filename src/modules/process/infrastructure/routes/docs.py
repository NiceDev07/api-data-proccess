"""
Documentación Swagger de la API.

- Metadatos globales (título, descripción, tags) → importados en main.py.
- Documentación de endpoints (schemas, ejemplos, responses) → importados en process.py y preview.py.
"""
import copy
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.models.confirm_dto import ConfirmRequest


# ── helpers ────────────────────────────────────────────────────────────────────

def _inline_schema_refs(schema: dict) -> dict:
    # Resuelve los $ref internos para que Swagger muestre el schema completo sin saltar a $defs
    schema = copy.deepcopy(schema)
    defs = schema.pop("$defs", {})

    def resolve(obj):
        if isinstance(obj, dict):
            if "$ref" in obj:
                ref_name = obj["$ref"].split("/")[-1]
                if ref_name in defs:
                    return resolve(copy.deepcopy(defs[ref_name]))
            return {k: resolve(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [resolve(item) for item in obj]
        return obj

    return resolve(schema)


# ── schemas inline ─────────────────────────────────────────────────────────────

PROCESSING_BODY_SCHEMA = _inline_schema_refs(DataProcessingDTO.model_json_schema())
CONFIRM_BODY_SCHEMA    = _inline_schema_refs(ConfirmRequest.model_json_schema())


# ── ejemplos ───────────────────────────────────────────────────────────────────

PROCESSING_EXAMPLES = {
    "sms_informativo": {
        "summary": "SMS — informativo",
        "value": {
            "content": "Estimado {nombre}, tiene una notificación pendiente.",
            "tariffId": 1,
            "campaignId": [999001],
            "codeGroup": "KXQM7291",
            "subService": "informative",
            "useExclusionList": False,
            "configListExclusion": None,
            "configFile": {
                "folder": "/ruta/al/archivo/campana.csv",
                "file": "campana.csv",
                "delimiter": ";",
                "useHeaders": True,
                "nameColumnDemographic": "telefono",
                "userIdentifier": False,
                "nameColumnIdentifier": "",
                "fileRecords": 5000,
            },
            "rulesCountry": {
                "idCountry": 1, "codeCountry": 57,
                "useCharacterSpecial": False,
                "limitCharacter": 160, "limitCharacterSpecial": 70,
                "numberDigitsMobile": 10, "numberDigitsFixed": 7,
                "useShortName": False,
            },
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        },
    },
    "email_standard": {
        "summary": "Email — standard",
        "value": {
            "content": "Estimado {nombre}, su solicitud ha sido procesada.",
            "subject": "Notificación de su cuenta",
            "tariffId": 1,
            "campaignId": [999002],
            "codeGroup": "BRTV4508",
            "subService": "standard",
            "useExclusionList": False,
            "configListExclusion": None,
            "configFile": {
                "folder": "/ruta/al/archivo/emails.xlsx",
                "file": "emails.xlsx",
                "delimiter": "",
                "useHeaders": True,
                "nameColumnDemographic": "email",
                "userIdentifier": False,
                "nameColumnIdentifier": "",
                "fileRecords": 3000,
            },
            "rulesCountry": {
                "idCountry": 1, "codeCountry": 57,
                "useCharacterSpecial": False,
                "limitCharacter": 160, "limitCharacterSpecial": 70,
                "numberDigitsMobile": 10, "numberDigitsFixed": 7,
                "useShortName": False,
            },
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        },
    },
    "call_blasting_standard": {
        "summary": "Call Blasting — standard (audioDuration)",
        "value": {
            "content": "",
            "tariffId": 1,
            "campaignId": [999003],
            "codeGroup": "LPWZ6134",
            "subService": "standard",
            "audioDuration": 20,
            "audioPath": None,
            "useExclusionList": False,
            "configListExclusion": None,
            "configFile": {
                "folder": "/ruta/al/archivo/campana.xlsx",
                "file": "campana.xlsx",
                "delimiter": "",
                "useHeaders": True,
                "nameColumnDemographic": "telefono",
                "userIdentifier": False,
                "nameColumnIdentifier": "",
                "fileRecords": 10000,
            },
            "rulesCountry": {
                "idCountry": 1, "codeCountry": 57,
                "useCharacterSpecial": False,
                "limitCharacter": 160, "limitCharacterSpecial": 70,
                "numberDigitsMobile": 10, "numberDigitsFixed": 7,
                "useShortName": False,
            },
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        },
    },
    "call_blasting_custom": {
        "summary": "Call Blasting — custom (mensaje personalizado por registro)",
        "value": {
            "content": "Estimado {nombre}, su saldo es {saldo} con fecha {fecha}.",
            "tariffId": 1,
            "campaignId": [999004],
            "codeGroup": "NFHJ8823",
            "subService": "custom",
            "audioDuration": None,
            "audioPath": None,
            "useExclusionList": False,
            "configListExclusion": None,
            "configFile": {
                "folder": "/ruta/al/archivo/campana_custom.xlsx",
                "file": "campana_custom.xlsx",
                "delimiter": "",
                "useHeaders": True,
                "nameColumnDemographic": "telefono",
                "userIdentifier": False,
                "nameColumnIdentifier": "",
                "fileRecords": 10000,
            },
            "rulesCountry": {
                "idCountry": 1, "codeCountry": 57,
                "useCharacterSpecial": False,
                "limitCharacter": 160, "limitCharacterSpecial": 70,
                "numberDigitsMobile": 10, "numberDigitsFixed": 7,
                "useShortName": False,
            },
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        },
    },
}

CONFIRM_EXAMPLES = {
    "sms": {
        "summary": "SMS",
        "value": {"campaignId": [229960], "codeGroup": "KXQM7291"},
    },
    "email": {
        "summary": "Email",
        "value": {"campaignId": [229961], "codeGroup": "BRTV4508"},
    },
    "call_blasting": {
        "summary": "Call Blasting",
        "value": {"campaignId": [229962], "codeGroup": "LPWZ6134"},
    },
}


# ── descripciones de rutas ─────────────────────────────────────────────────────

PROCESSING_DESCRIPTION = (
    "Lee el archivo CSV o XLSX indicado en configFile, valida cada registro según las reglas del servicio "
    "y guarda el resultado como Parquet. Ese Parquet es el que consume el endpoint de confirm.\n\n"
    "### Por servicio\n\n"
    "**SMS**\n"
    "- Valida longitud del número y asigna operador por rangos de numeración.\n"
    "- subService: informative o landing.\n"
    "- Si rulesCountry.useShortName es true, el campo shortname es obligatorio "
    "y debe estar incluido en el contenido del mensaje.\n\n"
    "**Email**\n"
    "- Valida el formato del correo y agrupa el resumen por dominio.\n"
    "- subService: standard.\n"
    "- El campo subject es obligatorio.\n\n"
    "**Call Blasting**\n"
    "- Valida el número, asigna operador y calcula duración y créditos del audio.\n"
    "- subService: standard o custom.\n"
    "- Para standard hay que enviar audioDuration en segundos o audioPath con la ruta al archivo de audio.\n"
    "- Para custom la duración se calcula desde el texto del mensaje, no se necesita audio previo.\n"
    "- Los números sin tarifa configurada quedan excluidos con razón NO_COST.\n\n"
    "### Notas\n"
    "- configFile.folder debe ser la ruta completa al archivo, no al directorio.\n"
    "- Usuario nivel 1: máximo 10 registros. Nivel 2 o superior: hasta 700 000.\n"
    "- El Parquet se identifica por campaignId o codeGroup. Ese mismo valor hay que enviarlo al confirm."
)

PROCESSING_RESPONSES = {
    200: {"description": "Procesamiento exitoso. Retorna summaryGeneral, summaryGroup y violations."},
    400: {"description": "Payload inválido: campo faltante, subService no permitido, shortname requerido, o audioDuration/audioPath faltante para call_blasting standard."},
    404: {"description": "Archivo de campaña no encontrado en la ruta indicada."},
    500: {"description": "Error interno del servidor."},
}

CONFIRM_DESCRIPTION = (
    "Toma el Parquet que dejó el endpoint de processing e inserta los registros válidos "
    "en la base de datos de campañas.\n\n"
    "### Antes de llamar este endpoint\n"
    "Hay que haber corrido processing con el mismo campaignId o codeGroup. "
    "Si el Parquet no existe retorna 404.\n\n"
    "### Por servicio\n\n"
    "**SMS** — Inserta en telefonos_campanas usando LOAD DATA LOCAL INFILE por lotes.\n\n"
    "**Email** — Crea la tabla mail_{campaignId} si no existe e inserta con INSERT IGNORE.\n\n"
    "**Call Blasting** — Crea la tabla en PostgreSQL e inserta con ON CONFLICT DO NOTHING.\n\n"
    "### Payload requerido\n"
    "- codeGroup: el mismo valor enviado en processing. Se usa para localizar el Parquet.\n"
    "- campaignId: lista con al menos un ID de campaña. Se usa para nombrar las tablas e insertar registros.\n\n"
    "### Cómo se busca el archivo\n"
    "Se busca el Parquet por codeGroup. Si no existe, se busca por los IDs de campaña concatenados."
)

CONFIRM_RESPONSES = {
    200: {"description": "Registros insertados. Retorna inserted con el total de filas confirmadas."},
    404: {"description": "Archivo Parquet no encontrado. Ejecute primero el endpoint de processing."},
    400: {"description": "Datos inválidos en el payload."},
    500: {"description": "Error interno del servidor."},
}


# ── first-rows ─────────────────────────────────────────────────────────────────

PREVIEW_DESCRIPTION = (
    "Lee las primeras 6 filas de un archivo CSV o XLSX y las retorna como lista de strings.\n\n"
    "Detecta automáticamente el encoding (`utf_8_sig`, `cp1252`, `latin1`) y el delimitador (`,` o `;`).\n\n"
    "El body debe contener exactamente los campos `folder` y `file`."
)

PREVIEW_RESPONSES = {
    200: {"description": "Vista previa generada correctamente."},
    400: {"description": "Body inválido o campos vacíos."},
    404: {"description": "Archivo no encontrado en la ruta indicada."},
    422: {"description": "Extensión de archivo no soportada. Solo se aceptan CSV y XLSX."},
    500: {"description": "Error interno del servidor."},
}

PREVIEW_OPENAPI_EXTRA = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["folder", "file"],
                    "properties": {
                        "folder": {"type": "string", "description": "Ruta absoluta al directorio que contiene el archivo."},
                        "file":   {"type": "string", "description": "Nombre del archivo con extensión (CSV o XLSX)."},
                    },
                },
                "examples": {
                    "csv": {
                        "summary": "Archivo CSV",
                        "value": {
                            "folder": "/home/dev_stefany/projects/Storage/data-process-prove",
                            "file": "test_callb_standard_100.csv",
                        },
                    },
                    "xlsx": {
                        "summary": "Archivo XLSX",
                        "value": {
                            "folder": "/home/dev_stefany/projects/Storage/data-process-prove",
                            "file": "test_email_50_prueba.xlsx",
                        },
                    },
                },
            }
        },
    }
}


# ── metadatos globales de la aplicación ───────────────────────────────────────

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
