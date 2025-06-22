from typing import List, Optional, Literal
from pydantic import BaseModel

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
    content: str
    shortname: str
    tariffId: int
    campaignId: List[int]
    configFile: ConfigFile
    useExclusionList: bool
    configListExclusion: ConfigListExclusion = None
    subService: str
    rulesCountry: RulesCountry
    infoUserValidSend: InfoUserValidSend
    # listBlockTerms: List[str] # YA DEBERIA APLICAR
    # listExclusionGeneral: List[str] # Ya no DBERIA APLICAR
