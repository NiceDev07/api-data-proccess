import os
from abc import ABC
from modules._common.domain.interfaces.file_reader import IFileReader

class BaseFileReader(IFileReader, ABC):
    def _check_file_exists(self, filepath: str):
        if not os.path.isfile(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")
