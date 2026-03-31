import polars as pl
from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.domain.enums.sub_services import SmsSubService

# (max_chars_per_part, overhead_for_multipart)
_PDU_STANDARD = (160, 7)
_PDU_SPECIAL = (70, 3)   # unicode / caracteres especiales

_URL_PATTERN = r"https?://\S+"
_LANDING_URL_PLACEHOLDER = "x" * 14  # longitud fija de URL acortada


class CalculatePDU(IPipeline):
    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        pdu_std, overhead_std = _PDU_STANDARD
        pdu_spc, overhead_spc = _PDU_SPECIAL

        msg = (
            pl.col(Cols.message).str.replace_all(_URL_PATTERN, _LANDING_URL_PLACEHOLDER)
            if ctx.subService == SmsSubService.landing
            else pl.col(Cols.message)
        )

        df = df.with_columns(
            msg.str.len_chars().alias(Cols.length),
            msg.str.len_bytes().alias(Cols.length_bytes),
        )

        df = df.with_columns(
            (pl.col(Cols.length) != pl.col(Cols.length_bytes)).alias(Cols.is_special)
        )

        df = df.with_columns(
            pl.when(pl.col(Cols.is_special))
            .then(pl.lit(pdu_spc))
            .otherwise(pl.lit(pdu_std))
            .alias(Cols.credit_base),

            pl.when(pl.col(Cols.is_special))
            .then(pl.lit(overhead_spc))
            .otherwise(pl.lit(overhead_std))
            .alias(Cols.overhead),
        )

        df = df.with_columns(
            pl.when(pl.col(Cols.length) <= pl.col(Cols.credit_base))
            .then(pl.col(Cols.credit_base))
            .otherwise(pl.col(Cols.credit_base) - pl.col(Cols.overhead))
            .alias(Cols.div)
        )

        return df.with_columns(
            (pl.col(Cols.length) / pl.col(Cols.div))
            .ceil()
            .cast(pl.Int32)
            .alias(Cols.pdu)
        )
