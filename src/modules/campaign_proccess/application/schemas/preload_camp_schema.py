from typing import List, Optional
from pydantic import BaseModel

# NO APLICA
class Cost(BaseModel):
    subService: str
    value: int

# NO APLICA
class Operator(BaseModel):
    name: str
    cost: List[Cost]
    prefix: str

# NO APLICA
class ListCostService(BaseModel):
    type: str
    operator: List[Operator]

# No APLICA
class PrefixItem(BaseModel):
    type: str
    prefix: List[str]

# SI APLICA
class RulesCountry(BaseModel):
    idCountry: int
    codeCountry: int
    prefix: List[PrefixItem] # YA NO APLICA
    useCharacterSpecial: bool
    limitCharacter: int
    limitCharacterSpecial: int
    numberDigitsMobile: int
    numberDigitsFixed: int
    listCostService: List[ListCostService] # YA NO APLICA
    useShortName: bool


class ConfigFile(BaseModel):
    folder: str
    file: str
    delimiter: str
    useHeaders: bool
    nameColumnDemographic: str
    userIdentifier: bool
    nameColumnIdentifier: str
    fileRecords: int


class ConfigListExclusion(BaseModel):
    folder: Optional[str]
    file: Optional[str]
    delimiter: Optional[str]
    useHeaders: Optional[bool]
    nameColumnDemographic: Optional[str]
    paramIdentifier: Optional[str]


class InfoUserValidSend(BaseModel):
    levelUser: int
    demographic: str


class PreloadCampDTO(BaseModel):
    content: str
    shortname: str
    tariffId: int
    campaignId: List[int]
    configFile: ConfigFile
    useExclusionList: bool
    configListExclusion: Optional[ConfigListExclusion]
    subService: str
    rulesCountry: RulesCountry
    infoUserValidSend: InfoUserValidSend
    listBlockTerms: List[str] # YA DEBERIA APLICAR
    listExclusionGeneral: List[str] # Ya no DBERIA APLICAR
