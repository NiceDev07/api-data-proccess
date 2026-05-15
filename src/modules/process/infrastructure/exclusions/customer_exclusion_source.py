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

        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        try:
            return lf.select(pl.col(col).alias(col)).collect()
        except pl.exceptions.ColumnNotFoundError:
            raise ValueError("COLUMN_NOT_FOUND: The demographic column was not found in the exclusion file.")

