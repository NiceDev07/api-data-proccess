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

        # Todas las derivaciones en un solo pass — evita 3 copias intermedias del DataFrame
        is_special = pl.col(Cols.length) != pl.col(Cols.length_bytes)
        credit_base = pl.when(is_special).then(pl.lit(pdu_spc)).otherwise(pl.lit(pdu_std))
        overhead = pl.when(is_special).then(pl.lit(overhead_spc)).otherwise(pl.lit(overhead_std))
        div = pl.when(pl.col(Cols.length) <= credit_base).then(credit_base).otherwise(credit_base - overhead)

        return df.with_columns(
            is_special.alias(Cols.is_special),
            credit_base.alias(Cols.credit_base),
            overhead.alias(Cols.overhead),
            div.alias(Cols.div),
            (pl.col(Cols.length) / div).ceil().cast(pl.Int32).alias(Cols.pdu),
        )
