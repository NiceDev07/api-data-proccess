"""Lógica compartida para extraer duración de audio via ffprobe."""

import json
import math
import os
import subprocess

from logging_config import get_logger

logger = get_logger(__name__)

_EXTRA_SECONDS = 5


def probe_duration(file_path: str, extra: int = _EXTRA_SECONDS) -> float:
    """Retorna ceil(duración) + extra segundos usando ffprobe (bloqueante).

    Args:
        file_path: Ruta absoluta al archivo de audio.
        extra: Segundos adicionales para agregar al resultado.

    Raises:
        RuntimeError: Si el archivo no existe, ffprobe no está instalado,
                      o el archivo es inválido/corrupto.
    """
    if not os.path.exists(file_path):
        # Log con el path para diagnóstico; el cliente solo ve el RuntimeError genérico.
        logger.debug("Audio no encontrado | path=%s", file_path)
        raise RuntimeError("El archivo de audio no existe.")

    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        info = json.loads(result.stdout)
        return math.ceil(float(info["format"]["duration"])) + extra

    except FileNotFoundError:
        logger.debug("ffprobe no instalado en el sistema")
        raise RuntimeError(
            "ffprobe no está instalado. Instálalo con 'sudo apt-get install ffmpeg'."
        )
    except subprocess.CalledProcessError as e:
        logger.debug("ffprobe falló al procesar | path=%s | stderr=%s", file_path, e.stderr.strip())
        raise RuntimeError(f"Error al ejecutar ffprobe: {e.stderr.strip()}")
    except (json.JSONDecodeError, KeyError, ValueError):
        logger.debug("Audio corrupto o no soportado | path=%s", file_path)
        raise RuntimeError(
            "Error al procesar el archivo de audio; puede estar corrupto o no ser soportado."
        )
