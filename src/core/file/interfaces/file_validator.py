from abc import ABC, abstractmethod

class FileValidatorInterface(ABC):
    @abstractmethod
    def validate(self, filepath: str) -> str:
        pass
