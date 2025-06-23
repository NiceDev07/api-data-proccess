import numpy as np
from datetime import datetime
from typing import Optional
from dataclasses import dataclass

@dataclass(frozen=True)
class TelcoCostInfo:
    prefixes: np.ndarray
    costs: np.ndarray
    lengths: np.ndarray
    date_cache: datetime
    initial: Optional[np.ndarray] = None
    incremental: Optional[np.ndarray] = None
