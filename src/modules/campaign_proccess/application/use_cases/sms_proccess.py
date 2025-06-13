from core.file.interfaces.file_reader import FileReaderInterface
from core.file.interfaces.file_validator import FileValidatorInterface
from modules.campaign_proccess.application.schemas.preload_camp_schema import PreloadCampDTO
from modules.campaign_proccess.application.helpers.tags import extract_tags_from_content
from modules.campaign_proccess.application.helpers.required_columns import build_required_columns

class SMSUseCase:
    def __init__(
        self,
        file_validator: FileValidatorInterface,
        file_reader: FileReaderInterface
    ):
        self.file_validator = file_validator
        self.file_reader = file_reader

    def execute(self, payload: PreloadCampDTO):
        file_path = payload.configFile.folder
        exists = self.file_validator.validate(file_path)
        if not exists:
            # Aqui podemos descargar el archivo desde un servicio externo
            raise FileNotFoundError(f"El archivo {file_path} no existe o no es accesible")
        content_tags = extract_tags_from_content(payload.content)
        usecols = build_required_columns(list(content_tags), payload.configFile)
        df = self.file_reader.read(file_path, usecols=usecols)

        # Validar Nivel de usuario
        # if payload.infoUserValidSend.levelUser <= 1:
        #    # Se muta df para que solo tenga 10 registros
        #     df = df.head(10)

        return {"success": True, "tags": content_tags}