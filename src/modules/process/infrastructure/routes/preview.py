from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

from modules.process.app.files.preview import get_first_rows
from modules.process.infrastructure.routes.docs import (
    PREVIEW_DESCRIPTION, PREVIEW_RESPONSES, PREVIEW_OPENAPI_EXTRA,
)
from config.settings import settings
from logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()

_ALLOWED_EXTENSIONS = ("csv", "xlsx")


class PreviewRequest(BaseModel):
    folder: str
    file: str

    @field_validator("folder", "file")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("FIELD_REQUIRED: Field cannot be empty.")
        return v

    @field_validator("file")
    @classmethod
    def validate_extension(cls, v: str) -> str:
        ext = v.rsplit(".", 1)[-1].lower() if "." in v else ""
        if ext not in _ALLOWED_EXTENSIONS:
            raise ValueError("INVALID_EXTENSION: Only CSV and XLSX files are supported.")
        return v


@router.post(
    "/first-rows",
    summary="Vista previa del archivo",
    description=PREVIEW_DESCRIPTION,
    responses=PREVIEW_RESPONSES,
    tags=["Files"],
    openapi_extra=PREVIEW_OPENAPI_EXTRA,
)
async def first_rows(payload: PreviewRequest):
    try:
        result = await get_first_rows(payload.folder, payload.file, settings.repository_files_dir)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("Error inesperado leyendo preview de '%s'", payload.file)
        raise HTTPException(status_code=500, detail="Error interno del servidor.")

    return JSONResponse(status_code=200, content=result)
