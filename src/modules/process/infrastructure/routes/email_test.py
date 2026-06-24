from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from modules.process.app.use_case.send_email_test import SendEmailTestUseCase
from modules.process.domain.models.process_dto import EmailTestRequest
from modules.process.infrastructure.errors import build_error_detail
from modules.process.infrastructure.routes.process_deps import get_send_email_test_use_case
from logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()


@router.post(
    "/send-email-test",
    summary="Enviar correo de prueba",
    description="Envía correos de prueba (1-5 destinatarios) con el contenido real de la campaña, reemplazando las etiquetas {tag} con los valores de las primeras filas del CSV.",
    tags=["Email Test"],
)
async def send_email_test(
    payload: EmailTestRequest,
    use_case: SendEmailTestUseCase = Depends(get_send_email_test_use_case),
):
    try:
        results = await use_case(payload)
        return JSONResponse(status_code=200, content={"success": True, "data": results})
    except FileNotFoundError as e:
        logger.warning("Email test | archivo no encontrado | path=%s | error=%s",
                       payload.configFile.folder, e)
        raise HTTPException(status_code=404, detail=build_error_detail(str(e)))
    except ValueError as e:
        logger.warning("Email test | error de validación | error=%s", e)
        raise HTTPException(status_code=400, detail=build_error_detail(str(e)))
    except Exception:
        logger.exception("Email test | error inesperado")
        raise HTTPException(
            status_code=500,
            detail=build_error_detail("INTERNAL_SERVER_ERROR: Internal server error."),
        )
