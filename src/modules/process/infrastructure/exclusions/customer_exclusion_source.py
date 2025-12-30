import polars as pl
from modules.process.app.interfaces.file_reader_factory import IFileReaderFactory
from modules.process.domain.models.process_dto import DataProcessingDTO

class CustomerExclusionSource:
    def __init__(self, reader: IFileReaderFactory):
        self.reader = reader

    async def get_df(self, ctx: DataProcessingDTO) -> pl.DataFrame:
        reader = self.reader.create(ctx.configListExclusion.folder)
        df = await reader.read(ctx.configListExclusion)
        col = ctx.configListExclusion.nameColumnDemographic

        return df.select(pl.col(col).alias("number")) # DEBE FUNCIONAR PARA MAIL

