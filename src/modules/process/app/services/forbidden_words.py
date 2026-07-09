import asyncio
import json

import ahocorasick
from cachetools import TTLCache

from modules._common.domain.interfaces.cache import ICache
from modules.process.domain.interfaces.forbidden_words_repository import IForbiddenWordsRepository
from modules.process.app.normalizers.text import normalize_for_filter
from logging_config import get_logger

logger = get_logger(__name__)

_REDIS_KEY = "forbidden_words:v1"
_REDIS_TTL = 43200  # 12 horas
_LRU_KEY = "filter_data"


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


def _scan_text(
    automaton: ahocorasick.Automaton, text: str, user_id: int
) -> list[tuple[str, int]]:
    """Recorre el autómata sobre el texto normalizado y devuelve (palabra, posición)
    de cada coincidencia que el usuario NO tenga autorizada. La posición es relativa
    al texto ya normalizado."""
    normalized = normalize_for_filter(text)
    hits: list[tuple[str, int]] = []
    for end_index, (term, authorized_users) in automaton.iter(normalized):
        if user_id in authorized_users:
            continue
        hits.append((term, end_index - len(term) + 1))
    return hits


class ForbiddenWordsService:
    def __init__(self, repo: IForbiddenWordsRepository, cache: ICache):
        self._repo = repo
        self._cache = cache
        # Un único autómata global vive en L1; basta con una entrada.
        self._lru: TTLCache = TTLCache(maxsize=1, ttl=1200)
        self._building: dict[str, asyncio.Future] = {}

    async def get_filter_data(self) -> tuple[ahocorasick.Automaton, dict[str, set[int]]]:
        cached = self._lru.get(_LRU_KEY)
        if cached is not None:
            logger.debug("ForbiddenWords | filtro servido desde L1 (RAM)")
            return cached

        try:
            redis_raw = await self._cache.get(_REDIS_KEY)
        except Exception:
            logger.warning("ForbiddenWords | fallo al leer Redis, se irá a BD directamente")
            redis_raw = None

        if redis_raw is not None:
            logger.debug("ForbiddenWords | filtro reconstruido desde Redis (L2)")
            terms = _json_to_terms(redis_raw)
            result = (_build_automaton(terms), terms)
            self._lru[_LRU_KEY] = result
            return result

        existing_future = self._building.get(_LRU_KEY)
        if existing_future is not None:
            logger.debug("ForbiddenWords | build en curso, esperando resultado (anti-thundering herd)")
            return await existing_future

        logger.info("ForbiddenWords | cache miss total (L1 y L2), cargando filtro desde BD")
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._building[_LRU_KEY] = future
        try:
            result = await self._load_from_db()
            # Poblamos L1 ANTES de liberar el building map: así las peticiones que
            # lleguen justo después encuentran el dato en L1 y no rehacen el query.
            self._lru[_LRU_KEY] = result
            future.set_result(result)
            return result
        except Exception as exc:
            future.set_exception(exc)
            raise
        finally:
            self._building.pop(_LRU_KEY, None)

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
            logger.warning("ForbiddenWords | fallo al escribir en Redis; L1 sigue activo")
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

    async def invalidate_cache(self) -> None:
        self._lru.clear()
        try:
            await self._cache.delete(_REDIS_KEY)
        except Exception:
            logger.warning("ForbiddenWords | fallo al borrar clave Redis en invalidación")
        logger.info("ForbiddenWords | cache invalidado (L1 + Redis)")
