from abc import ABC, abstractmethod

class IProcess(ABC):
    @abstractmethod
    def process(self):
        pass