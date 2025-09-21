from enum import Enum

class Status(str, Enum):
    WRONG = "C"
    PENDING = "P"

class ResultProcessing(str, Enum):
    EXCLUDED_RNE = "ESME_BLOCK_CRC"