from fastapi import APIRouter, Depends, HTTPException, Request

from modules.process.app.services.forbidden_words import ForbiddenWordsService
from modules.process.domain.models.filter_dto import (
    BlockedMessage,
    BlockedWord,
    InvalidateCacheRequest,
    InvalidateCacheResponse,
    ValidateCampaignRequest,
    ValidateCampaignResponse,
    ValidateTextRequest,
    ValidateTextResponse,
)
from modules.process.infrastructure.errors import build_error_detail
from config.settings import settings
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


@router.post(
    "/filtro/validar-campana",
    summary="Validar lista de mensajes contra palabras prohibidas",
    response_model=ValidateCampaignResponse,
)
async def validate_campaign(
    req: ValidateCampaignRequest,
    service: ForbiddenWordsService = Depends(_get_forbidden_words_service),
):
    try:
        bloqueados: list[BlockedMessage] = []
        for i, mensaje in enumerate(req.mensajes):
            hits = await service.validate_text(mensaje, req.user_id)
            if hits:
                palabra, _ = hits[0]
                bloqueados.append(BlockedMessage(indice=i, mensaje=mensaje, palabra=palabra))

        return ValidateCampaignResponse(
            total=len(req.mensajes),
            aprobados=len(req.mensajes) - len(bloqueados),
            bloqueados=bloqueados,
        )
    except Exception as exc:
        logger.exception("Error inesperado en /filtro/validar-campana")
        raise HTTPException(
            status_code=500,
            detail=build_error_detail(f"INTERNAL_SERVER_ERROR: {exc}"),
        )


@router.post(
    "/filtro/invalidar-cache",
    summary="Invalidar cache del filtro de palabras prohibidas",
    response_model=InvalidateCacheResponse,
)
async def invalidate_cache(
    req: InvalidateCacheRequest,
    service: ForbiddenWordsService = Depends(_get_forbidden_words_service),
):
    if req.secret != settings.filter_cache_secret:
        raise HTTPException(status_code=403, detail=build_error_detail("FORBIDDEN: Secret inválido."))
    try:
        await service.invalidate_cache()
        logger.info("Cache de palabras prohibidas invalidado manualmente")
        return InvalidateCacheResponse(invalidado=True)
    except Exception as exc:
        logger.exception("Error inesperado en /filtro/invalidar-cache")
        raise HTTPException(
            status_code=500,
            detail=build_error_detail(f"INTERNAL_SERVER_ERROR: {exc}"),
        )
