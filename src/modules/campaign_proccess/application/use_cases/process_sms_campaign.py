from core.file_readers.csv_reader import CSVReader
from modules.campaign_proccess.domain.interfaces.proccess import ProcessorInterface
# from modules.campaign_proccess.domain.entities.sms_informativo_processor import SMSInformativoProcessor
# from modules.campaign_proccess.domain.entities.sms_landing_processor import SMSLandingProcessor

class SMSCampaignProcessorUseCase:
    def __init__(self, reader: CSVReader):
        self.reader = reader

    def execute(self, filepath: str, regulation: dict, subservice: str, user_level: int):
        nrows = 10 if user_level == 1 else None
        df = self.reader.read(filepath, usecols=None, nrows=nrows)

        processor: ProcessorInterface

        # if subservice == "informativo":
        #     processor = SMSInformativoProcessor(df, regulation)
        # elif subservice == "landing":
        #     processor = SMSLandingProcessor(df, regulation)
        # else:
        #     raise ValueError(f"Subservicio '{subservice}' no es soportado")

        return processor.process()
