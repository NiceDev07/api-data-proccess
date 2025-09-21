from abc import ABC, abstractmethod

class IBlackListCRCRepository(ABC):
    @abstractmethod
    async def get_black_list_crc(self, country_id: int) -> list[int]:
        """Retrieve the CRC black list for a specific user."""
        pass