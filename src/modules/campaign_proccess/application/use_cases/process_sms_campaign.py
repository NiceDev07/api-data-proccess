from core.file_readers.factory import FileReaderFactory

# Importa los procesadores (o importa en otro módulo y registra)
# from modules.campaign_proccess.domain.services.sms_informativo_processor import SMSInformativoProcessor
# from modules.campaign_proccess.domain.services.sms_landing_processor import SMSLandingProcessor


class SMSCampaignProcessorUseCase:
    # Registro dinámico para subservicios y sus procesadores
    _processor_registry = {
        # "informativo": SMSInformativoProcessor,
        # "landing": SMSLandingProcessor,
        # agregar más subservicios aquí
    }

    def __init__(self):
        pass

    def execute(self, filepath: str, regulation: dict, subservice: str, user_level: int):
        reader = FileReaderFactory.get_reader(filepath)
        nrows = 10 if user_level == 1 else None
        df = reader.read(filepath, usecols=None, nrows=nrows)
        print(f"Procesando archivo: {filepath} con {len(df)} filas")
        print(df.head())
        # processor_cls = self._processor_registry.get(subservice)
        # if not processor_cls:
        #     raise ValueError(f"Subservicio '{subservice}' no es soportado")

        # processor: ProcessorInterface = processor_cls(df, regulation)
        # return processor.process()
