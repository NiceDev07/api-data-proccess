import re

import polars as pl

from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.domain.constants.cols import Cols

_TAG_RE = re.compile(r"\{(\w+(?:-\d+)?)\}")


class CustomSubject(IPipeline):
    """Personaliza el asunto del correo por registro.

    Si el asunto contiene etiquetas ({tag}), sustituye cada una por el valor
    de la columna correspondiente en el DataFrame, igual que CustomMessage.
    Si no hay etiquetas, asigna el texto literal a todos los registros.
    """

    async def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        if not ctx.subject:
            raise ValueError("El campo 'subject' es obligatorio para el servicio email.")

        ordered_tags = _TAG_RE.findall(ctx.subject)

        if not ordered_tags:
            return df.with_columns(pl.lit(ctx.subject).alias(Cols.subject))

        template = _TAG_RE.sub("{}", ctx.subject)
        return df.with_columns(
            pl.format(template, *[pl.col(tag).cast(pl.Utf8) for tag in ordered_tags])
            .alias(Cols.subject)
        )
