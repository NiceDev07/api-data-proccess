import polars as pl

from modules.process.domain.interfaces.pipeline import IPipeline
from modules.process.domain.models.process_dto import DataProcessingDTO
from modules.process.app.pipelines.shared._template import build_template_expr, _TAG_RE
from modules.process.app.services.forbidden_words import ForbiddenWordsService
from logging_config import get_logger

logger = get_logger(__name__)

_CHECK_COL = "__forbidden_check__"


class ValidateForbiddenWordsStep(IPipeline):
    """Valida palabras prohibidas en el contenido de la campaña.

    Primero revisa la plantilla estática; si tiene tags de personalización,
    construye el mensaje final por fila (vectorizado, con la misma normalización
    de tags que CustomMessage) y valida todos los mensajes en un solo lote.
    """

    def __init__(self, service: ForbiddenWordsService):
        self._service = service

    async def execute(
        self, df: pl.DataFrame | pl.LazyFrame, ctx: DataProcessingDTO
    ) -> pl.DataFrame | pl.LazyFrame:
        content = ctx.content or ""
        user_id = ctx.infoUserValidSend.userId
        logger.debug("ForbiddenWords | validando contenido de campaña | user_id=%s", user_id)

        template_hits = await self._service.validate_text(content, user_id)
        if template_hits:
            words = ", ".join(t for t, _ in template_hits)
            logger.warning(
                "ForbiddenWords | contenido bloqueado | user_id=%s | palabras=%s", user_id, words
            )
            raise ValueError(
                f"text-invalid: El mensaje contiene palabras no permitidas: {words}"
            )

        # Sin tags no hay personalización: la plantilla ya quedó validada arriba.
        if not _TAG_RE.search(content):
            logger.debug("ForbiddenWords | contenido sin tags validado sin coincidencias | user_id=%s", user_id)
            return df

        actual_df = df.collect() if isinstance(df, pl.LazyFrame) else df
        messages = (
            actual_df
            .select(build_template_expr(content, _CHECK_COL, actual_df.columns))
            .get_column(_CHECK_COL)
            .to_list()
        )

        words = await self._service.find_first_violation(messages, user_id)
        if words:
            joined = ", ".join(words)
            logger.warning(
                "ForbiddenWords | registro bloqueado | user_id=%s | palabras=%s", user_id, joined
            )
            raise ValueError(
                "FORBIDDEN_WORD_IN_RECORD: El mensaje personalizado contiene "
                f"palabras no permitidas: {joined}"
            )

        logger.debug(
            "ForbiddenWords | contenido y %d registros validados sin coincidencias | user_id=%s",
            len(messages), user_id,
        )
        return actual_df
