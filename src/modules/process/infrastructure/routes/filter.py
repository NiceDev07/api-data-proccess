from fastapi import APIRouter, Depends, HTTPException, Request

from modules.process.app.services.forbidden_words import ForbiddenWordsService
from modules.process.domain.models.filter_dto import (
    BlockedWord,
    ValidateTextRequest,
    ValidateTextResponse,
)
from modules.process.infrastructure.errors import build_error_detail
from logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Filter"])


def _get_forbidden_words_service(request: Request) -> ForbiddenWordsService:
    return request.app.state.process_deps.forbidden_words_service


@router.post(
    "/filtro/validar",
    summary="Validar texto contra palabras prohibidas",
    response_model=ValidateTextResponse,
)
async def validate_text(
    req: ValidateTextRequest,
    service: ForbiddenWordsService = Depends(_get_forbidden_words_service),
):
    try:
        hits = await service.validate_text(req.texto, req.user_id)
        return ValidateTextResponse(
            permitido=len(hits) == 0,
            palabras_bloqueadas=[BlockedWord(palabra=t, posicion=p) for t, p in hits],
        )
    except Exception as exc:
        logger.exception("Error inesperado en /filtro/validar")
        raise HTTPException(
            status_code=500,
            detail=build_error_detail(f"INTERNAL_SERVER_ERROR: {exc}"),
        )
