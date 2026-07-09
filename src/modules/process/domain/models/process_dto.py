from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from modules.process.domain.enums.sub_services import SmsSubService, CallBlastingSubService
from modules.process.domain.utils import normalize_col_name


class RulesCountry(BaseModel):
    idCountry: int = Field(..., description="ID del país en la base de numeración.", examples=[81])
    codeCountry: int = Field(..., description="Código telefónico del país (sin +).", examples=[57])
    useCharacterSpecial: bool = Field(..., description="Si el país permite caracteres especiales (unicode) en SMS.")
    limitCharacter: int = Field(..., description="Límite de caracteres para SMS estándar (ASCII).", examples=[160])
    limitCharacterSpecial: int = Field(..., description="Límite de caracteres para SMS unicode.", examples=[70])
    numberDigitsMobile: int = Field(..., description="Cantidad de dígitos de un número móvil válido.", examples=[10])
    numberDigitsFixed: int = Field(..., description="Cantidad de dígitos de un número fijo válido.", examples=[7])
    useShortName: bool = Field(..., description="Si es `true`, el campo `shortname` es obligatorio y debe estar incluido en `content`.")

    @property
    def national_digits(self) -> int:
        """Dígitos del número nacional (el mayor entre móvil y fijo).

        Criterio único de "número nacional" compartido por ConcatPrefix y
        LevelValidator para que ambos concuerden en cómo arman/comparan el número.
        """
        return max(self.numberDigitsMobile, self.numberDigitsFixed)


class BaseFileConfig(BaseModel):
    folder: str = Field(..., description="Ruta absoluta al directorio que contiene el archivo.")
    file: str = Field(..., description="Nombre del archivo con extensión (CSV o XLSX).")
    delimiter: str = Field(..., description="Delimitador de columnas para CSV. Para XLSX enviar vacío — el campo no se usa en la lectura.")
    useHeaders: bool = Field(..., description="Si `true`, la primera fila se usa como encabezado.")
    nameColumnDemographic: str = Field(..., description="Nombre de la columna que contiene el número/email.")
    n_rows: int | None = Field(default=None, description="Límite de filas a leer. None = sin límite (lectura completa).")

    @field_validator("nameColumnDemographic")
    @classmethod
    def _normalize_demographic(cls, v: str) -> str:
        return normalize_col_name(v)


class ConfigFile(BaseFileConfig):
    userIdentifier: bool = Field(..., description="Si `true`, el archivo incluye columna de identificación.")
    nameColumnIdentifier: str = Field(..., description="Nombre de la columna de identificación. Requerido si `userIdentifier=true`.")
    fileRecords: int = Field(..., description="Total de registros en el archivo declarado por el cliente (sin contar encabezado). Es metadata informativa — el sistema valida el conteo real al leer el archivo.", examples=[200000])

    @field_validator("nameColumnIdentifier")
    @classmethod
    def _normalize_identifier(cls, v: str) -> str:
        return normalize_col_name(v)


class ConfigListExclusion(BaseFileConfig):
    paramIdentifier: Optional[Literal['demographic', 'identifier']] = Field(
        None,
        description="Columna a usar para comparar exclusiones: `demographic` (número/email) o `identifier`.",
    )


class InfoUserValidSend(BaseModel):
    levelUser: int = Field(..., description="Nivel del usuario: `1` = pruebas (máx. 10 registros), `2+` = producción (máx. 700 000).", examples=[2])
    demographic: str = Field(default='', description="Número o email del usuario para validación de nivel 1. Vacío en nivel 2+ o si el emisor no lo envía (default defensivo: evita acoplar el despliegue).")
    userId: int = Field(0, description="ID del usuario que crea la campaña.")


class ConfigLabel(BaseModel):
    nameLabel: str = Field(..., description="Nombre del placeholder tal como aparece en `content` entre llaves.")
    typeLabel: str = Field(..., description="Tipo TTS del valor: `N` (nombre), `S` (string), `M` (monto), etc. Usar `undefined` para no aplicar tipo.")


class DataProcessingDTO(BaseModel):
    # Requerido para SMS y call_blasting custom (plantilla del mensaje).
    # Opcional para call_blasting standard (usa audio, no texto).
    content: Optional[str] = Field(None, description="Plantilla del mensaje. Puede incluir etiquetas `{columna}` para personalización. Requerido para SMS y call_blasting custom.", examples=["Hola {nombre}, tu código es {codigo}."])
    shortname: Optional[str] = Field(None, description="Remitente SMS. Obligatorio solo cuando `rulesCountry.useShortName=true` y debe estar incluido en `content`.")
    tariffId: int = Field(..., description="ID de la tarifa a aplicar para el cálculo de créditos.", examples=[1])
    campaignId: List[int] = Field(default_factory=list, description="IDs de campaña. Opcional cuando se envía codeGroup.", examples=[[999001]])
    codeGroup: str = Field(..., description="Identificador de grupo para nombrar el archivo Parquet. Tiene prioridad sobre campaignId. Mínimo 8 caracteres.")

    @field_validator("codeGroup")
    @classmethod
    def validate_code_group(cls, v: str) -> str:
        if len(v.strip()) < 8:
            raise ValueError("INVALID_CODE_GROUP: codeGroup must be at least 8 characters.")
        return v
    configFile: ConfigFile = Field(..., description="Configuración del archivo de datos a procesar.")
    useExclusionList: bool = Field(..., description="Si `true`, se aplica la lista de exclusión definida en `configListExclusion`.")
    configListExclusion: Optional[ConfigListExclusion] = Field(None, description="Configuración del archivo de exclusión. Requerido si `useExclusionList=true`.")
    subService: str = Field(..., description="Sub-servicio a aplicar. Valores válidos según el servicio: SMS → `informative` | `landing`; Email → `standard`; Call Blasting → `standard` | `custom`.")
    rulesCountry: RulesCountry = Field(..., description="Reglas del país: prefijos, límites de caracteres y validación de números.")
    infoUserValidSend: InfoUserValidSend = Field(..., description="Información del nivel de usuario para validación de capacidad.")
    # Solo flujo UNITARIO: el SP consulta_operador_pais los usa para resolver el routing.
    # El masivo los ignora (el routing lo hace el daemon externo al enviar).
    useFlash: bool = Field(default=False, description="SMS flash. Unitario: selecciona el código corto flash en el routing.")
    gCode: int = Field(default=1, description="Grupo de códigos cortos para el routing (unitario).")
    subject: Optional[str] = Field(None, description="Asunto del correo. Obligatorio para el servicio `email`.")
    audioPath: Optional[str] = Field(None, description="Ruta local al archivo de audio. Requerido para `call_blasting standard`.")
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
    - Para `standard`: se requiere `audioPath` (ruta al archivo de audio).
    - Para `custom`: se requiere `content` para el mensaje TTS.
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
            if not self.audioPath:
                raise ValueError("AUDIO_PATH_REQUIRED: audioPath is required for call_blasting standard sub-service.")
        if self.subService == CallBlastingSubService.custom.value:
            if not self.content or not self.content.strip():
                raise ValueError("CONTENT_REQUIRED: content is required for call_blasting custom sub-service.")
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
        if not self.content or not self.content.strip():
            raise ValueError("CONTENT_REQUIRED: content is required for SMS.")

        if (
            self.rulesCountry.useShortName
            and (not self.shortname or not self.shortname.strip())
        ):
            raise ValueError("SHORTNAME_REQUIRED: Shortname is required.")

        return self


class EmailRecipient(BaseModel):
    demographic: str = Field(..., description="Correo destino al que se envía la prueba.")
    status: bool = Field(..., description="Si `false`, el destinatario se omite del envío.")


class EmailTestRequest(BaseModel):
    """
    Payload para `POST /v2/send-email-test`.

    Envía correos de prueba a una lista de destinatarios (1-5) con el contenido
    real de la campaña, reemplazando las etiquetas `{tag}` con los valores de
    las primeras filas del CSV.
    """

    subService: str = Field(..., description="Sub-servicio de email aplicado a la prueba.")
    content: str = Field(..., description="HTML del correo con etiquetas `{tag}` a reemplazar desde el CSV.")
    subject: str = Field(..., description="Asunto del correo con etiquetas `{tag}` opcionales.")
    listDemographics: List[EmailRecipient] = Field(..., description="Destinatarios de prueba (1-5 elementos).")
    configFile: ConfigFile = Field(..., description="Configuración del archivo CSV de la campaña.")

    @field_validator("listDemographics")
    @classmethod
    def _validate_count(cls, v: List[EmailRecipient]) -> List[EmailRecipient]:
        if not (1 <= len(v) <= 5):
            raise ValueError("INVALID_RECIPIENTS_COUNT: listDemographics must have between 1 and 5 elements.")
        return v
