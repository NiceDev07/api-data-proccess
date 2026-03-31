import polars as pl
from modules.process.domain.interfaces.regulation import IRegulation
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols


class ShortNameRegulation(IRegulation):
    def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> None:
        if not ctx.rulesCountry.useShortName:
            return

        # Fast path: el shortname está en la plantilla → todos los mensajes lo tienen
        if ctx.shortname in ctx.content:
            return

        # El shortname viene de un tag dinámico: validar cada mensaje del DataFrame
        all_contain = df.select(
            pl.col(Cols.message).str.contains(ctx.shortname, literal=True).all()
        ).item()

        if not all_contain:
            raise ValueError(
                f"El mensaje debe contener el shortname '{ctx.shortname}'."
            )

class SpecialCharRegulation(IRegulation):
    def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> None:
        if not ctx.rulesCountry.useCharacterSpecial and df[Cols.is_special].any():
            raise ValueError(
                "El país de destino no permite caracteres especiales en el mensaje."
            )

class CharLimitRegulation(IRegulation):
    def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> None:
        rules = ctx.rulesCountry
        limit_expr = (
            pl.when(pl.col(Cols.is_special))
            .then(pl.lit(rules.limitCharacterSpecial))
            .otherwise(pl.lit(rules.limitCharacter))
        )
        if df.select((pl.col(Cols.length) > limit_expr).any()).item():
            raise ValueError(
                f"Uno o más mensajes superan el límite de caracteres permitido "
                f"({rules.limitCharacter} estándar / {rules.limitCharacterSpecial} especial)."
            )


SMS_REGULATIONS: list[IRegulation] = [
    ShortNameRegulation(),
    SpecialCharRegulation(),
    CharLimitRegulation(),
]
