"""
Logging centralizado de la aplicación.

Uso:
    from logging_config import setup_logging, get_logger
    setup_logging()                          # una vez, en main.py o lifespan
    logger = get_logger(__name__)            # en cada módulo

Niveles:
    - DEBUG   : trazas de desarrollo (desactivado en producción)
    - INFO    : eventos del flujo normal (pipeline start/end, guardado de archivo)
    - WARNING : situaciones anómalas recuperables (Redis caído, registro sin operador)
    - ERROR   : errores que afectan una operación específica
    - CRITICAL: fallo que impide que el servicio funcione
"""
import logging
import os
import sys
from typing import Optional

# ── ANSI ──────────────────────────────────────────────────────────────────────
_RESET  = "\033[0m"
_BOLD   = "\033[1m"
_DIM    = "\033[2m"

_LEVEL_STYLES: dict[str, str] = {
    "DEBUG":    "\033[36m",        # cyan
    "INFO":     "\033[32m",        # green
    "WARNING":  "\033[33m",        # yellow
    "ERROR":    "\033[31m",        # red
    "CRITICAL": "\033[35;1m",      # magenta bold
}

_FMT    = "%(asctime)s %(levelname)-8s %(name)-40s %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _use_colors() -> bool:
    """Activa colores si el stream es un TTY o si FORCE_COLOR está definido."""
    return bool(os.getenv("FORCE_COLOR")) or (
        hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
    )


class _ColorFormatter(logging.Formatter):
    def __init__(self, colors: bool = True) -> None:
        super().__init__(fmt=_FMT, datefmt=_DATEFMT)
        self._colors = colors

    def format(self, record: logging.LogRecord) -> str:
        msg = super().format(record)
        if not self._colors:
            return msg

        style = _LEVEL_STYLES.get(record.levelname, "")
        # Colorear solo el nivel en el string formateado
        plain_level = f"{record.levelname:<8}"
        colored_level = f"{style}{_BOLD}{plain_level}{_RESET}"
        # Atenuar el nombre del logger para no saturar visualmente
        logger_name = record.name.ljust(40)[:40]
        dim_name = f"{_DIM}{logger_name}{_RESET}"

        msg = msg.replace(plain_level, colored_level, 1)
        msg = msg.replace(logger_name, dim_name, 1)
        return msg


def setup_logging(level: Optional[int] = None) -> None:
    """
    Configura el sistema de logging.
    Llamar una sola vez al iniciar la aplicación.

    Args:
        level: nivel de logging (por defecto logging.INFO).
               En dev se puede pasar logging.DEBUG.
    """
    if level is None:
        level = logging.DEBUG if os.getenv("ENV", "prod") == "dev" else logging.INFO

    colors = _use_colors()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_ColorFormatter(colors=colors))
    handler.setLevel(level)

    root = logging.getLogger()
    root.setLevel(level)
    # Evitar handlers duplicados si setup_logging se llama más de una vez
    root.handlers = [handler]

    # ── Silenciar librerías verbosas en producción ────────────────────────────
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("asyncmy").setLevel(logging.ERROR)


def get_logger(name: str) -> logging.Logger:
    """Devuelve un logger con el nombre indicado (usar __name__ en cada módulo)."""
    return logging.getLogger(name)
