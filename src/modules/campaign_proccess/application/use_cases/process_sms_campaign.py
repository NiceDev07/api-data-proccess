from core.file.readers.factory import FileReaderFactory
from modules.campaign_proccess.domain.interfaces.proccess import ProcessorInterface
from modules.campaign_proccess.domain.services.sms.sms_base_proccessor import SMSInformativoProcessor

class SMSCampaignProcessorUseCase:
    # Registro dinámico para subservicios y sus procesadores
    _processor_registry = {
        "informative": SMSInformativoProcessor,
        # "landing": SMSLandingProcessor,
        # agregar más subservicios aquí
    }

    def __init__(self):
        pass

    def execute(self, filepath: str, regulation: dict, user_level: int, subservice: str = "informative"):
        reader = FileReaderFactory.get_reader(filepath)
        nrows = 10 if user_level == 1 else None
        df = reader.read(filepath, usecols=None, nrows=nrows)
        processor_cls = self._processor_registry.get(subservice)
        if not processor_cls:
            raise ValueError(f"Subservicio '{subservice}' no es soportado")

        processor: ProcessorInterface = processor_cls({})
        return processor.process(df)


# use_case = SMSCampaignProcessorUseCase()

# df = use_case.execute(
#     filepath="/home/esteban/saem/refactors/api-data-proccess/sms_campaign_file.csv",
#     regulation={"some": "regulation"},
#     user_level=1,
#     subservice="informative"
# )

# print(df.head())  # Imprime las primeras filas del DataFrame procesado