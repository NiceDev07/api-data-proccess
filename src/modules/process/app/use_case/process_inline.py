from modules.process.app.files.inline_reader import InlineReader
from modules.process.app.interfaces import IProcessorFactory
from modules.process.domain.interfaces.level_validator import IUserLevelValidator
from modules.process.domain.enums.services import ServiceType
from modules.process.domain.models.inline_dto import InlineSmsRequest
from modules.process.domain.models.process_dto import SmsDataProcessingDTO, ConfigFile
from modules.process.domain.constants.cols import Cols

# Columna lógica interna donde el pipeline lee el número (equivale al header del
# archivo en el flujo masivo). Valor fijo — la entrada inline no tiene archivo.
_INLINE_COLUMN = "phone"
# codeGroup sintético (≥8 chars) — no se persiste (no hay Parquet en el flujo inline).
_INLINE_CODE_GROUP = "inlineunit"


class ProcessSmsInlineUseCase:
    """Flujo UNITARIO: recibe números en el request, corre el MISMO pipeline SMS del
    masivo (vía InlineReader + SmsProcessor.process_rows) y devuelve resultado por número.
    No escribe Parquet ni envía SMS — eso lo orquesta el MSA sms."""

    def __init__(self, processor_factory: IProcessorFactory, level_validator: IUserLevelValidator):
        self.processor_factory = processor_factory
        self.level_validator = level_validator

    async def __call__(self, req: InlineSmsRequest) -> list[dict]:
        payload = self._build_dto(req)
        reader = InlineReader(req.numbers)
        lf = await reader.read(payload.configFile)
        lf = await self.level_validator.validate(lf, payload)
        processor = self.processor_factory.create(ServiceType.sms)
        df = await processor.process_rows(lf, payload)
        return self._to_rows(df)

    @staticmethod
    def _build_dto(req: InlineSmsRequest) -> SmsDataProcessingDTO:
        # DTO sintético: reutiliza el contrato del masivo con un configFile "dummy"
        # (los campos de archivo no se usan porque el InlineReader trae los datos).
        config = ConfigFile(
            folder="",
            file="",
            delimiter=",",
            useHeaders=False,
            nameColumnDemographic=_INLINE_COLUMN,
            userIdentifier=False,
            nameColumnIdentifier=_INLINE_COLUMN,
            fileRecords=len(req.numbers),
        )
        return SmsDataProcessingDTO(
            content=req.content,
            shortname=req.shortname,
            tariffId=req.tariffId,
            campaignId=[],
            codeGroup=_INLINE_CODE_GROUP,
            configFile=config,
            useExclusionList=False,
            subService=req.subService,
            rulesCountry=req.rulesCountry,
            infoUserValidSend=req.infoUserValidSend,
            useFlash=req.useFlash,
            gCode=req.gCode,
        )

    @staticmethod
    def _to_rows(df) -> list[dict]:
        # Routing resuelto por el SP (operador + código corto + API + servidor +
        # portabilidad) → el MSA solo persiste y envía, sin re-resolver nada.
        return [
            {
                "number":     row[Cols.number_concat],   # con prefijo país (57...)
                "national":   row[_INLINE_COLUMN],        # número nacional (para el envío Jasmind)
                "operator":   row[Cols.number_operator],
                "operatorId": row[Cols.operator_id],
                "shortCode":  row[Cols.short_code],
                "userApi":    row[Cols.user_api],
                "server":     row[Cols.server],
                "pdu":        row[Cols.pdu],
                "cost":       row[Cols.cost],
                "credits":    row[Cols.credits],
                "isOk":       row[Cols.is_ok],
                "errorCode":  row[Cols.error_code],
            }
            for row in df.iter_rows(named=True)
        ]
