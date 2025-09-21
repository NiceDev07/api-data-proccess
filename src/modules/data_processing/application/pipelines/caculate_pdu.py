from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO

class CalculatePDU(IPipeline):
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
            pl.col('__message__').str.len_chars().alias('__length__'),
            pl.col('__message__').str.len_bytes().alias('__length_bytes__')
        )

        df = df.with_columns(
            (pl.col('__length__') != pl.col('__length_bytes__')).alias('__is_character_special__')
        )

        df = df.with_columns([
            # Paso 1: credit_base según si es especial
            pl.when(pl.col("__is_character_special__"))
            .then(pl.lit(pdu_special))
            .otherwise(pl.lit(pdu_standard))
            .alias("__credit_base__"),

            # Paso 2: overhead
            pl.when(pl.col("__is_character_special__"))
            .then(pl.lit(overhead_special))
            .otherwise(pl.lit(overhead_standard))
            .alias("__overhead__")
        ])

        # Paso 3: divisor
        df = df.with_columns(
            pl.when(pl.col("__length__") <= pl.col("__credit_base__"))
            .then(pl.col("__credit_base__"))
            .otherwise(pl.col("__credit_base__") - pl.col("__overhead__"))
            .alias("__divisor__")
        )

        # Paso 4: calcular PDUs
        df = df.with_columns(
            (pl.col("__length__") / pl.col("__divisor__"))
            .ceil()
            .cast(pl.Int32)
            .alias("__pdu_total__")
        )
        
        return df