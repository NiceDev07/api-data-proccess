from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.domain.constants.cols import Cols

class CalculatePDU(IPipeline):
    def __init__(
        self,
        cols: Cols = Cols()
    ):
        self.cols = cols

    # Toca que este configurable 
    # LEN, OVERHEAD
    PDU = {
        "standard": (160, 7),
        "special": (70, 3)
    }
    PDU_STANDARD = 160
    OVERHEAD_STANDARD = 7
    PDU_SPECIAL = 70
    OVERHEAD_SPECIAL = 3
        

    def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        pdu_standard, overhead_standard = self.PDU["standard"]
        pdu_special, overhead_special = self.PDU["special"]

        df = df.with_columns(
            pl.col(self.cols.message).str.len_chars().alias(self.cols.length),
            pl.col(self.cols.message).str.len_bytes().alias(self.cols.length_bytes)
        )

        df = df.with_columns(
            (pl.col(self.cols.length) != pl.col(self.cols.length_bytes)).alias(self.cols.is_special)
        )

        df = df.with_columns([
            # Paso 1: credit_base según si es especial
            pl.when(pl.col(self.cols.is_special))
            .then(pl.lit(pdu_special))
            .otherwise(pl.lit(pdu_standard))
            .alias(self.cols.credit_base),

            # Paso 2: overhead
            pl.when(pl.col(self.cols.is_special))
            .then(pl.lit(overhead_special))
            .otherwise(pl.lit(overhead_standard))
            .alias(self.cols.overhead)
        ])

        # Paso 3: divisor
        df = df.with_columns(
            pl.when(pl.col(self.cols.length) <= pl.col(self.cols.credit_base))
            .then(pl.col(self.cols.credit_base))
            .otherwise(pl.col(self.cols.credit_base) - pl.col(self.cols.overhead))
            .alias(self.cols.div)
        )

        # Paso 4: calcular PDUs
        df = df.with_columns(
            (pl.col(self.cols.length) / pl.col(self.cols.div))
            .ceil()
            .cast(pl.Int32)
            .alias(self.cols.pdu)
        )
        
        return df