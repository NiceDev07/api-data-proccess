from ..interfaces.file_validator import FileValidatorInterface
import os

class LocalFileValidator(FileValidatorInterface):
    def validate(self, filepath: str) -> str:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")
        return filepath
