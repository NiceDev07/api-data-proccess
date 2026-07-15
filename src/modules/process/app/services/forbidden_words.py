import asyncio
import json

import ahocorasick

from modules._common.domain.interfaces.cache import ICache
from modules.process.domain.interfaces.forbidden_words_repository import IForbiddenWordsRepository
from modules.process.app.normalizers.text import normalize_for_filter
from logging_config import get_logger

logger = get_logger(__name__)

_REDIS_KEY = "forbidden_words:v1"
_REDIS_TTL = 1800  # 30 minutos


def _build_automaton(terms: dict[str, set[int]]) -> ahocorasick.Automaton:
    A: ahocorasick.Automaton = ahocorasick.Automaton()
    for term, authorized_users in terms.items():
        normalized = normalize_for_filter(term)
        if normalized:
            A.add_word(normalized, (normalized, authorized_users))
    if len(A) > 0:
        A.make_automaton()
    return A


def _terms_to_json(terms: dict[str, set[int]]) -> str:
    payload = [{"t": term, "users": list(users)} for term, users in terms.items()]
    return json.dumps({"terms": payload}, ensure_ascii=False, separators=(",", ":"))


def _json_to_terms(raw: object) -> dict[str, set[int]]:
    if not isinstance(raw, dict) or "terms" not in raw:
        return {}
    return {entry["t"]: set(entry["users"]) for entry in raw["terms"]}


def _is_word_boundary_match(normalized: str, start_index: int, end_index: int) -> bool:
    """El match debe ser una palabra completa, no un fragmento dentro de otra
    palabra distinta (ej. "pene" dentro de "penerasta", "culo" dentro de
    "vehiculo"). El único separador que sobrevive a la normalización es el
    espacio, así que un límite válido es el inicio/fin del texto o un espacio."""
    left_ok = start_index == 0 or normalized[start_index - 1] == " "
    right_ok = end_index == len(normalized) - 1 or normalized[end_index + 1] == " "
    return left_ok and right_ok


def _scan_text(
    automaton: ahocorasick.Automaton, text: str, user_id: int
) -> list[tuple[str, int]]:
    """Recorre el autómata sobre el texto normalizado y devuelve (palabra, posición)
    de cada coincidencia que el usuario NO tenga autorizada. La posición es relativa
    al texto ya normalizado. Descarta coincidencias que sean solo un fragmento
    interno de una palabra distinta (ver _is_word_boundary_match)."""
    normalized = normalize_for_filter(text)
    hits: list[tuple[str, int]] = []
    for end_index, (term, authorized_users) in automaton.iter(normalized):
        start_index = end_index - len(term) + 1
        if not _is_word_boundary_match(normalized, start_index, end_index):
            continue
        if user_id in authorized_users:
            continue
        hits.append((term, start_index))
    return hits


class ForbiddenWordsService:
    def __init__(self, repo: IForbiddenWordsRepository, cache: ICache):
        self._repo = repo
        self._cache = cache
        self._building: dict[str, asyncio.Future] = {}

    async def get_filter_data(self) -> tuple[ahocorasick.Automaton, dict[str, set[int]]]:
        try:
            redis_raw = await self._cache.get(_REDIS_KEY)
        except Exception:
            logger.warning("ForbiddenWords | fallo al leer Redis, se irá a BD directamente")
            redis_raw = None

        if redis_raw is not None:
            logger.debug("ForbiddenWords | filtro reconstruido desde Redis")
            terms = _json_to_terms(redis_raw)
            return (_build_automaton(terms), terms)

        existing_future = self._building.get(_REDIS_KEY)
        if existing_future is not None:
            logger.debug("ForbiddenWords | build en curso, esperando resultado (anti-thundering herd)")
            return await existing_future

        logger.info("ForbiddenWords | cache miss en Redis, cargando filtro desde BD")
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._building[_REDIS_KEY] = future
        try:
            result = await self._load_from_db()
            future.set_result(result)
            return result
        except Exception as exc:
            future.set_exception(exc)
            raise
        finally:
            self._building.pop(_REDIS_KEY, None)

    async def _load_from_db(self) -> tuple[ahocorasick.Automaton, dict[str, set[int]]]:
        try:
            terms = await self._repo.get_active_terms()
        except Exception as exc:
            logger.error("ForbiddenWords | fallo al cargar filtro desde BD | error=%s", exc)
            raise RuntimeError(
                "FILTER_DB_ERROR: No se pudo cargar el filtro de palabras."
            ) from exc

        automaton = _build_automaton(terms)
        logger.info(
            "ForbiddenWords | filtro cargado desde BD | términos=%d | en autómata=%d",
            len(terms), len(automaton),
        )

        result = (automaton, terms)
        try:
            await self._cache.set(_REDIS_KEY, json.loads(_terms_to_json(terms)), _REDIS_TTL)
        except Exception:
            logger.warning("ForbiddenWords | fallo al escribir en Redis")
        return result

    async def validate_text(self, text: str, user_id: int) -> list[tuple[str, int]]:
        automaton, _ = await self.get_filter_data()
        if len(automaton) == 0:
            return []
        return _scan_text(automaton, text, user_id)

    async def find_first_violation(self, texts: list[str], user_id: int) -> list[str] | None:
        """Escanea una lista de textos y devuelve las palabras prohibidas del PRIMER
        texto que contenga alguna (None si todos están limpios). Carga el filtro una
        sola vez y corre el escaneo en un hilo para no bloquear el event loop."""
        automaton, _ = await self.get_filter_data()
        if len(automaton) == 0:
            return None

        def _scan() -> list[str] | None:
            for text in texts:
                hits = _scan_text(automaton, text, user_id)
                if hits:
                    return [term for term, _ in hits]
            return None

        return await asyncio.to_thread(_scan)
