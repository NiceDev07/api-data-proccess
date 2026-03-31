import re
import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.enums.sub_services import SmsSubService

_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


class Landing(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        if ctx.subService != SmsSubService.landing:
            return df

        if not _URL_RE.search(ctx.content):
            raise ValueError("El contenido del mensaje debe incluir una URL para subservicios de tipo landing.")

        return df
