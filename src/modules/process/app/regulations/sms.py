import polars as pl
from modules.process.domain.interfaces.regulation import IRegulation
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason


class ShortNameRegulation(IRegulation):
    def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        if not ctx.rulesCountry.useShortName:
            return df

        missing = ~(
            pl.col(Cols.message)
            .str.to_lowercase()
            .str.contains(ctx.shortname.lower(), literal=True)
        )

        to_mark = pl.col(Cols.is_ok) & missing

        result = df.with_columns(
            pl.when(to_mark)
            .then(pl.lit(False))
            .otherwise(pl.col(Cols.is_ok))
            .alias(Cols.is_ok),

            pl.when(to_mark)
            .then(pl.lit(ExclusionReason.SHORTNAME_MISSING))
            .otherwise(pl.col(Cols.error_code))
            .alias(Cols.error_code),
        )

        # All-or-nothing: si algún registro carece del shortname, abortamos toda
        # la campaña. No tiene sentido enviar parcial — el gateway downstream
        # rechaza los mensajes sin el shortname registrado y el cliente termina
        # pagando por envíos que nunca llegan.
        has_missing = result.filter(
            pl.col(Cols.error_code) == ExclusionReason.SHORTNAME_MISSING
        ).height > 0
        if has_missing:
            raise ValueError(
                "SHORTNAME_REQUIRED_IN_ALL: All records must contain the required "
                "shortname to send the campaign."
            )

        return result


class SpecialCharRegulation(IRegulation):
    def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        if ctx.rulesCountry.useCharacterSpecial:
            return df

        to_mark = pl.col(Cols.is_ok) & pl.col(Cols.is_special)

        return df.with_columns(
            pl.when(to_mark).then(pl.lit(False)).otherwise(pl.col(Cols.is_ok))
            .alias(Cols.is_ok),
            pl.when(to_mark)
            .then(pl.lit(ExclusionReason.SPECIAL_CHAR_NOT_ALLOWED))
            .otherwise(pl.col(Cols.error_code))
            .alias(Cols.error_code),
        )


# CharLimitRegulation — DESACTIVADA. Se mantiene comentada por si se reactiva en el
# futuro. Razón: el flujo activo del data-process anterior no rechazaba mensajes
# largos — CalculatePDU los cobra como multi-parte (ceil(length / 153) ASCII).
#
# class CharLimitRegulation(IRegulation):
#     def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
#         rules = ctx.rulesCountry
#         limit_expr = (
#             pl.when(pl.col(Cols.is_special))
#             .then(pl.lit(rules.limitCharacterSpecial))
#             .otherwise(pl.lit(rules.limitCharacter))
#         )
#         exceeds = pl.col(Cols.length) > limit_expr
#         to_mark = pl.col(Cols.is_ok) & exceeds
#
#         return df.with_columns(
#             pl.when(to_mark).then(pl.lit(False)).otherwise(pl.col(Cols.is_ok))
#             .alias(Cols.is_ok),
#             pl.when(to_mark)
#             .then(pl.lit(ExclusionReason.CHAR_LIMIT_EXCEEDED))
#             .otherwise(pl.col(Cols.error_code))
#             .alias(Cols.error_code),
#         )


SMS_REGULATIONS: list[IRegulation] = [
    ShortNameRegulation(),
    SpecialCharRegulation(),
    # CharLimitRegulation(),  # desactivada — ver comentario arriba
]
