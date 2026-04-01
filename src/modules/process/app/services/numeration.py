import numpy as np
from modules._common.domain.interfaces.cache import ICache

class NumerationService:
    KEY_CACHE = "numeration:v1"
    TTL_SECONDS = 60*60*2

    def __init__(
        self,
        numeration_repo,
        cache: ICache
    ):
        self.numeration_repo = numeration_repo
        self.cache = cache

    def get_key_cache(self, country_id: int) -> str:
        return f"{self.KEY_CACHE}:{country_id}"

    async def get_ranges(self, country_id: int):
        key_cache = self.get_key_cache(country_id)
        cached = await self.cache.get(key_cache)

        if cached is not None:
            return (
                np.array(cached["starts"], dtype=np.int64),
                np.array(cached["ends"], dtype=np.int64),
                np.array(cached["operators"], dtype=object),
            )

        ranges = await self.numeration_repo.get_numeracion(country_id)
        sorted_ranges = sorted(ranges, key=lambda r: r[0])
        starts = np.array([r[0] for r in sorted_ranges], dtype=np.int64)
        ends = np.array([r[1] for r in sorted_ranges], dtype=np.int64)
        operators = np.array([r[2] for r in sorted_ranges], dtype=object)

        await self.cache.set(
            key_cache,
            {"starts": starts.tolist(), "ends": ends.tolist(), "operators": operators.tolist()},
            self.TTL_SECONDS,
        )

        return starts, ends, operators