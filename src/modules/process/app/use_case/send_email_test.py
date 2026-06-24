import re
from typing import Any, Dict, List

from modules.process.app.files.factory import ReaderFileFactory
from modules.process.domain.models.process_dto import EmailTestRequest
from modules.process.domain.utils import normalize_col_name

# Regex de email igual al pipeline ValidateEmail — mantiene la validación consistente.
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

# Captura {tag} sin llaves anidadas.
_TAG_RE = re.compile(r"\{([^{}]+)\}")


def _is_valid_email(value: str) -> bool:
    return bool(_EMAIL_RE.match(value or ""))


def _replace_tags(template: str, row: Dict[str, Any]) -> str:
    # Reemplaza solo las {tag} que existan en la fila; deja el resto tal cual.
    # Más seguro que str.format, que lanza KeyError si falta una clave.
    return _TAG_RE.sub(lambda m: str(row.get(m.group(1), m.group(0))), template)


class SendEmailTestUseCase:
    def __init__(self, sender, file_reader_factory: ReaderFileFactory):
        self._sender         = sender
        self._reader_factory = file_reader_factory

    async def __call__(self, payload: EmailTestRequest) -> List[Dict[str, Any]]:
        # Marcamos cada destinatario como válido (status=true + email con formato correcto).
        recipients_validity = [
            (r, r.status and _is_valid_email(r.demographic))
            for r in payload.listDemographics
        ]
        valid_count = sum(1 for _, ok in recipients_validity if ok)

        if valid_count == 0:
            raise ValueError("NO_VALID_RECIPIENTS: No valid recipients found for sending.")

        # Solo leemos N filas (las que necesitamos) — evita cargar archivos grandes.
        # preview=True: valores como string sin inferir tipos para que los enteros
        # del CSV no aparezcan en el correo con ".0" al final.
        config = payload.configFile.model_copy(update={"n_rows": valid_count})
        reader = self._reader_factory.create(config.file, preview=True)
        df = (await reader.read(config)).collect()
        # Normaliza los headers (sin acentos, minúsculas) para que las {tag} del cliente
        # matcheen con las columnas del CSV sin importar capitalización. Los valores siguen
        # como string gracias a preview=True (no salen con ".0").
        df = df.rename({c: normalize_col_name(c) for c in df.columns})

        results: List[Dict[str, Any]] = []
        valid_idx = 0
        for recipient, is_valid in recipients_validity:
            if not is_valid:
                results.append({
                    "mobile":     recipient.demographic,
                    "valid":      False,
                    "valid_send": False,
                    "content":    "",
                    "subject":    "",
                })
                continue

            # Si hay más destinatarios válidos que filas en el CSV, reutilizamos la última.
            row_idx = min(valid_idx, df.height - 1) if df.height > 0 else None
            row     = df.row(row_idx, named=True) if row_idx is not None else {}
            content = _replace_tags(payload.content, row)
            subject = _replace_tags(payload.subject, row)

            sent = await self._sender.send(recipient.demographic, subject, content)
            results.append({
                "mobile":     recipient.demographic,
                "valid":      True,
                "valid_send": sent,
                "content":    content,
                "subject":    subject,
            })
            valid_idx += 1

        return results
