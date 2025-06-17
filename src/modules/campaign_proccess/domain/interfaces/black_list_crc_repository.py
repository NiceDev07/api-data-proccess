from abc import ABC, abstractmethod

class IBlackListCRCRepository(ABC):
    @abstractmethod
    def get_black_list_crc(self) -> list[int]:
        """Retrieve the CRC black list for a specific user."""
        pass