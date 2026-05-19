from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from modules.process.domain.enums.sub_services import SmsSubService, CallBlastingSubService




class RulesCountry(BaseModel):
    idCountry: int = Field(..., description="ID del país en la base de numeración.", examples=[81])
    codeCountry: int = Field(..., description="Código telefónico del país (sin +).", examples=[57])
    useCharacterSpecial: bool = Field(..., description="Si el país permite caracteres especiales (unicode) en SMS.")
    limitCharacter: int = Field(..., description="Límite de caracteres para SMS estándar (ASCII).", examples=[160])
    limitCharacterSpecial: int = Field(..., description="Límite de caracteres para SMS unicode.", examples=[70])
    numberDigitsMobile: int = Field(..., description="Cantidad de dígitos de un número móvil válido.", examples=[10])
    numberDigitsFixed: int = Field(..., description="Cantidad de dígitos de un número fijo válido.", examples=[7])
    useShortName: bool = Field(..., description="Si es `true`, el campo `shortname` es obligatorio y debe estar incluido en `content`.")


class BaseFileConfig(BaseModel):
    folder: str = Field(..., description="Ruta absoluta al directorio que contiene el archivo.")
    file: str = Field(..., description="Nombre del archivo con extensión (CSV o XLSX).")
    delimiter: str = Field(..., description="Delimitador de columnas para CSV. Dejar vacío para XLSX.")
    useHeaders: bool = Field(..., description="Si `true`, la primera fila se usa como encabezado.")
    nameColumnDemographic: str = Field(..., description="Nombre de la columna que contiene el número/email.")


class ConfigFile(BaseFileConfig):
    userIdentifier: bool = Field(..., description="Si `true`, el archivo incluye columna de identificación.")
    nameColumnIdentifier: str = Field(..., description="Nombre de la columna de identificación. Requerido si `userIdentifier=true`.")
    fileRecords: int = Field(..., description="Total de registros en el archivo declarado por el cliente (sin contar encabezado). Es metadata informativa — el sistema valida el conteo real al leer el archivo.", examples=[200000])


class ConfigListExclusion(BaseFileConfig):
    paramIdentifier: Optional[Literal['demographic', 'identifier']] = Field(
        None,
        description="Columna a usar para comparar exclusiones: `demographic` (número/email) o `identifier`.",
    )


class InfoUserValidSend(BaseModel):
    levelUser: int = Field(..., description="Nivel del usuario: `1` = pruebas (máx. 10 registros), `2+` = producción (máx. 700 000).", examples=[2])
    demographic: str = Field(..., description="Número o email del usuario para validación de nivel 1. Vacío en nivel 2+.")


class ConfigLabel(BaseModel):
    nameLabel: str = Field(..., description="Nombre del placeholder tal como aparece en `content` entre llaves.")
    typeLabel: str = Field(..., description="Tipo TTS del valor: `N` (nombre), `S` (string), `M` (monto), etc. Usar `undefined` para no aplicar tipo.")


class DataProcessingDTO(BaseModel):
    content: str = Field(..., description="Plantilla del mensaje. Puede incluir etiquetas `{columna}` para personalización.", examples=["Hola {nombre}, tu código es {codigo}."])
    shortname: Optional[str] = Field(None, description="Remitente SMS. Obligatorio solo cuando `rulesCountry.useShortName=true` y debe estar incluido en `content`.")
    tariffId: int = Field(..., description="ID de la tarifa a aplicar para el cálculo de créditos.", examples=[1])
    campaignId: List[int] = Field(..., description="Lista de IDs de campaña. Se usa como nombre del archivo Parquet resultante.", examples=[[999001]])
    codeGroup: Optional[str] = Field(None, description="Clave de grupo alternativa para nombrar el archivo Parquet. Tiene prioridad sobre `campaignId`. Mínimo 8 caracteres si se provee.")

    @field_validator("campaignId")
    @classmethod
    def validate_campaign_id(cls, v: List[int]) -> List[int]:
        if not v:
            raise ValueError("CAMPAIGN_ID_REQUIRED: campaignId must contain at least one ID.")
        return v

    @field_validator("codeGroup")
    @classmethod
    def validate_code_group(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and len(v.strip()) < 8:
            raise ValueError("INVALID_CODE_GROUP: codeGroup must be at least 8 characters.")
        return v
    configFile: ConfigFile = Field(..., description="Configuración del archivo de datos a procesar.")
    useExclusionList: bool = Field(..., description="Si `true`, se aplica la lista de exclusión definida en `configListExclusion`.")
    configListExclusion: Optional[ConfigListExclusion] = Field(None, description="Configuración del archivo de exclusión. Requerido si `useExclusionList=true`.")
    subService: str = Field(..., description="Sub-servicio a aplicar. Valores válidos según el servicio: SMS → `informative` | `landing`; Email → `standard`; Call Blasting → `standard` | `custom`.")
    rulesCountry: RulesCountry = Field(..., description="Reglas del país: prefijos, límites de caracteres y validación de números.")
    infoUserValidSend: InfoUserValidSend = Field(..., description="Información del nivel de usuario para validación de capacidad.")
    subject: Optional[str] = Field(None, description="Asunto del correo. Obligatorio para el servicio `email`.")
    audioDuration: Optional[float] = Field(None, description="Duración del audio en segundos. Requerido para `call_blasting` sub-servicio `standard` si no se provee `audioPath`.")
    audioPath: Optional[str] = Field(None, description="Ruta local al archivo de audio. Alternativa a `audioDuration` para `call_blasting standard`.")
    configLabels: List[ConfigLabel] = Field(
        default_factory=list,
        description=(
            "Tipado TTS por placeholder. Exclusivo de `call_blasting` `custom`. "
            "Cada entrada indica cómo debe pronunciar el motor TTS el valor de ese campo. "
            "Ejemplo: `{nombre}` con tipo `N` se convierte en `{N:Carlos}` en el mensaje final. "
            "Enviar lista vacía `[]` cuando no se requiere tipado TTS."
        ),
    )


class CallBlastingDataProcessingDTO(DataProcessingDTO):
    """
    Payload para `POST /v2/processing/call_blasting`.

    Extiende `DataProcessingDTO` con validaciones específicas de call blasting:
    - `subService` debe ser `"standard"` o `"custom"`.
    - Para `standard`: se requiere `audioDuration` o `audioPath`.
    """

    @field_validator("subService")
    @classmethod
    def subservice_valid(cls, v: str) -> str:
        if v not in {s.value for s in CallBlastingSubService}:
            raise ValueError("INVALID_SUB_SERVICE: sub-service not allowed for call blasting.")
        return v

    @model_validator(mode="after")
    def validate_audio_source(self) -> "CallBlastingDataProcessingDTO":
        if self.subService == CallBlastingSubService.standard.value:
            if self.audioDuration is None and not self.audioPath:
                raise ValueError("AUDIO_SOURCE_REQUIRED: audioDuration or audioPath is required for standard sub-service.")
        return self


class SmsDataProcessingDTO(DataProcessingDTO):
    """
    Payload para `POST /v2/processing/sms`.

    Extiende `DataProcessingDTO` con validaciones específicas de SMS:
    - `subService` debe ser `"informative"` o `"landing"`.
    - `shortname` es obligatorio cuando `rulesCountry.useShortName=true` y debe estar incluido en `content`.
    """

    @field_validator("subService")
    @classmethod
    def subservice_valid(cls, v: str) -> str:
        if v not in {s.value for s in SmsSubService}:
            raise ValueError("INVALID_SUB_SERVICE: sub-service not allowed for SMS.")
        return v

    @model_validator(mode="after")
    def validate_shortname(self) -> "SmsDataProcessingDTO":
        if not self.rulesCountry.useShortName:
            return self
        if not self.shortname or not self.shortname.strip():
            raise ValueError("SHORTNAME_REQUIRED: Shortname is required.")
        if self.shortname not in self.content:
            raise ValueError("SHORTNAME_NOT_IN_CONTENT: Message content must include the shortname.")
        return self
