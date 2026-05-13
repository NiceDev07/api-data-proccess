from typing import List, Optional, Literal
from pydantic import BaseModel, field_validator, model_validator
from modules.process.domain.enums.sub_services import SmsSubService

# SI APLICA
class RulesCountry(BaseModel):
    idCountry: int
    codeCountry: int
    # prefix: List[PrefixItem] # YA NO APLICA
    useCharacterSpecial: bool
    limitCharacter: int
    limitCharacterSpecial: int
    numberDigitsMobile: int
    numberDigitsFixed: int
    # listCostService: List[ListCostService] # YA NO APLICA
    useShortName: bool

class BaseFileConfig(BaseModel):
    folder: str
    file: str
    delimiter: str
    useHeaders: bool
    nameColumnDemographic: str

class ConfigFile(BaseFileConfig):
    userIdentifier: bool
    nameColumnIdentifier: str
    fileRecords: int

class ConfigListExclusion(BaseFileConfig):
    paramIdentifier: Optional[Literal['demographic', 'identifier']] = None

class InfoUserValidSend(BaseModel):
    levelUser: int
    demographic: str


class DataProcessingDTO(BaseModel):
    content: str # Tener en cuenta para el hash
    shortname: Optional[str] = None
    tariffId: int # Tener en cuenta para el hash
    campaignId: List[int]
    codeGroup: Optional[str] = None
    configFile: ConfigFile # Tener en cuenta para el hash
    useExclusionList: bool # Tener en cuenta para el hash
    configListExclusion: Optional[ConfigListExclusion] = None # Tener en cuenta para el hash
    subService: str # Tener en cuenta para el hash
    rulesCountry: RulesCountry # Tener en cuenta para el hash
    infoUserValidSend: InfoUserValidSend # Tener en cuenta para el hash
    subject: Optional[str] = None          # asunto del correo; obligatorio para email
    audioDuration: Optional[float] = None  # segundos; requerido en call_blasting_standard
    audioPath: Optional[str] = None        # ruta local o URL; alternativa a audioDuration
    # listBlockTerms: List[str] # YA DEBERIA APLICAR
    # listExclusionGeneral: List[str] # Ya no DBERIA APLICAR


class SmsDataProcessingDTO(DataProcessingDTO):

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
