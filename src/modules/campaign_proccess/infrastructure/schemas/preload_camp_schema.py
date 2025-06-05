from typing import List, Optional
from pydantic import BaseModel


class Cost(BaseModel):
    subService: str
    value: int


class Operator(BaseModel):
    name: str
    cost: List[Cost]
    prefix: str


class ListCostService(BaseModel):
    type: str
    operator: List[Operator]


class PrefixItem(BaseModel):
    type: str
    prefix: List[str]


class RulesCountry(BaseModel):
    idCountry: int
    codeCountry: int
    prefix: List[PrefixItem]
    useCharacterSpecial: bool
    limitCharacter: int
    limitCharacterSpecial: int
    numberDigitsMobile: int
    numberDigitsFixed: int
    listCostService: List[ListCostService]
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


class MainModel(BaseModel):
    content: str
    shortname: str
    tariffId: int
    campaignId: List[int]
    configFile: ConfigFile
    useExclusionList: bool
    configListExclusion: ConfigListExclusion
    subService: str
    rulesCountry: RulesCountry
    infoUserValidSend: InfoUserValidSend
    listBlockTerms: List[str]
    listExclusionGeneral: List[str]
