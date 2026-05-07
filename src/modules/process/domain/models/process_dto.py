from typing import List, Optional, Literal
from pydantic import BaseModel, field_validator

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
    shortname: str

    @field_validator("shortname")
    @classmethod
    def shortname_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("El shortname no puede estar vacío.")
        return v
