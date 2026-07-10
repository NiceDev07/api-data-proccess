"""
Tests unitarios para ForbiddenWordsService.

Cubre caching L1/L2, anti-thundering herd, degradación graceful,
lógica de permisos por usuario y normalización de texto.
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from modules.process.app.services.forbidden_words import ForbiddenWordsService, _REDIS_KEY, _LRU_KEY

pytestmark = pytest.mark.anyio


# ── helpers ───────────────────────────────────────────────────────────────────

def make_cache_mock(redis_data: object = None) -> MagicMock:
    mock = MagicMock()
    mock.get = AsyncMock(return_value=redis_data)
    mock.set = AsyncMock()
    mock.delete = AsyncMock()
    return mock


def make_repo_mock(terms: list[tuple[str, set[int]]] | None = None) -> MagicMock:
    mock = MagicMock()
    mock.get_active_terms = AsyncMock(return_value=dict(terms or []))
    return mock


def _redis_payload(terms: dict[str, set[int]]) -> dict:
    return {
        "terms": [{"t": t, "users": list(u)} for t, u in terms.items()]
    }


def make_service(
    repo_terms: list[tuple[str, set[int]]] | None = None,
    redis_data: object = None,
) -> tuple[ForbiddenWordsService, MagicMock, MagicMock]:
    cache = make_cache_mock(redis_data)
    repo = make_repo_mock(repo_terms)
    svc = ForbiddenWordsService(repo, cache)
    return svc, repo, cache


# ── tests ─────────────────────────────────────────────────────────────────────

async def test_blocked_word_for_user_without_permission():
    """Palabra global (set vacío) → bloqueada para cualquier usuario."""
    svc, repo, _ = make_service(repo_terms=[("droga", set())])
    hits = await svc.validate_text("compra droga aqui", user_id=999)
    assert len(hits) > 0
    assert any(t == "droga" for t, _ in hits)


async def test_blocked_word_authorized_user_is_allowed():
    """Palabra con excepción para user_id 101 → permitida para ese usuario."""
    svc, repo, _ = make_service(repo_terms=[("arma", {101, 202})])
    hits = await svc.validate_text("vendo arma legal", user_id=101)
    assert hits == []


async def test_blocked_word_non_authorized_user_is_blocked():
    """Misma palabra con excepción → bloqueada para user_id no en la lista."""
    svc, repo, _ = make_service(repo_terms=[("arma", {101, 202})])
    hits = await svc.validate_text("vendo arma legal", user_id=999)
    assert len(hits) > 0


async def test_clean_text_returns_empty():
    """Texto sin palabras prohibidas → lista vacía."""
    svc, repo, _ = make_service(repo_terms=[("droga", set())])
    hits = await svc.validate_text("hola mundo oferta especial", user_id=1)
    assert hits == []


async def test_l1_cache_hit_skips_repo():
    """Hit en L1 (TTLCache en RAM) → repo no se llama."""
    svc, repo, cache = make_service(repo_terms=[("palabra", set())])

    await svc.validate_text("texto", user_id=1)
    first_call_count = repo.get_active_terms.call_count

    await svc.validate_text("texto", user_id=1)
    assert repo.get_active_terms.call_count == first_call_count


async def test_l2_redis_cache_hit_skips_repo():
    """Hit en Redis (L2) con L1 vacío → repo no se llama."""
    redis_payload = _redis_payload({"forbidden": set()})
    svc, repo, cache = make_service(redis_data=redis_payload)

    await svc.validate_text("forbidden word", user_id=5)
    repo.get_active_terms.assert_not_called()


async def test_full_cache_miss_calls_repo_and_saves():
    """Miss total → repo se llama, resultado guardado en Redis y L1."""
    svc, repo, cache = make_service(repo_terms=[("test", set())])

    await svc.get_filter_data()

    repo.get_active_terms.assert_called_once()
    cache.set.assert_called_once()
    call_args = cache.set.call_args
    assert call_args[0][0] == _REDIS_KEY

    assert _LRU_KEY in svc._lru


async def test_thundering_herd_repo_called_once():
    """Dos corrutinas con cache miss simultáneo → repo se llama exactamente 1 vez."""
    call_count = 0
    original_terms = {"palabra": set()}

    async def slow_get():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return original_terms

    cache = make_cache_mock(redis_data=None)
    repo = MagicMock()
    repo.get_active_terms = slow_get
    svc = ForbiddenWordsService(repo, cache)

    results = await asyncio.gather(
        svc.get_filter_data(),
        svc.get_filter_data(),
    )

    assert call_count == 1
    automaton1, terms1 = results[0]
    automaton2, terms2 = results[1]
    assert terms1 == terms2


async def test_redis_unavailable_falls_back_to_db():
    """Redis lanza excepción en lectura → servicio sigue funcionando vía DB."""
    cache = MagicMock()
    cache.get = AsyncMock(side_effect=Exception("Redis connection refused"))
    cache.set = AsyncMock()
    repo = make_repo_mock([("forbidden", set())])

    svc = ForbiddenWordsService(repo, cache)
    hits = await svc.validate_text("forbidden text", user_id=1)

    assert len(hits) > 0
    repo.get_active_terms.assert_called_once()


async def test_inactive_word_not_in_automaton():
    """activo=0 implica que el repo no lo retorna → la palabra no aparece en hits."""
    # El repo ya filtra activo=1 — simulamos que no retorna "inactiva"
    svc, repo, _ = make_service(repo_terms=[("activa", set())])
    hits = await svc.validate_text("inactiva palabra", user_id=1)
    assert all(t != "inactiva" for t, _ in hits)


async def test_normalization_catches_variants():
    """Normalización atrapa variantes con mayúsculas, tildes y separadores."""
    svc, _, _ = make_service(repo_terms=[("droga", set())])

    hits_upper = await svc.validate_text("DROGA", user_id=1)
    assert len(hits_upper) > 0, "Mayúsculas no detectadas"

    svc2, _, _ = make_service(repo_terms=[("droga", set())])
    svc2._lru.clear()
    hits_tilde = await svc2.validate_text("dróga", user_id=1)
    assert len(hits_tilde) > 0, "Tilde no normalizada"

    svc3, _, _ = make_service(repo_terms=[("droga", set())])
    svc3._lru.clear()
    hits_sep = await svc3.validate_text("dr-oga", user_id=1)
    assert len(hits_sep) > 0, "Separador no eliminado"


async def test_invalidate_cache_clears_l1_and_redis():
    """invalidate_cache limpia L1 y borra la clave Redis."""
    svc, repo, cache = make_service(repo_terms=[("word", set())])
    await svc.get_filter_data()
    assert _LRU_KEY in svc._lru

    await svc.invalidate_cache()

    assert _LRU_KEY not in svc._lru
    cache.delete.assert_called_once_with(_REDIS_KEY)
