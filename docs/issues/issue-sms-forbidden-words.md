# GitLab Issue

**Título:** [FEAT](SMS) Módulo de filtrado de palabras prohibidas con Aho-Corasick y caché L1/L2

## Objetivo

- El servicio data-process no validaba palabras prohibidas en campañas SMS; la lógica vivía en el MSA SMS sobre un esquema legacy (`autorizado`/`id_autorizados` + stored procedure `np_valid_content_sms`), difícil de mantener y con riesgo de SQL injection.
- Construir un módulo nuevo, desacoplado y escalable, que centralice el filtrado de blacklist usando Aho-Corasick, con caché en dos niveles (L1 en RAM + Redis L2), degradación graceful a BD y permisos de excepción por `user_id`.
- Exponerlo como endpoint HTTP para que el gateway lo consuma (SMS unitario) y como paso del pipeline para la campaña masiva.

## Resultado Esperado

- [x] Paso `ValidateForbiddenWordsStep` integrado en el pipeline SMS (cubre la campaña masiva vía `/processing/sms`); siempre activo.
- [x] Endpoints `POST /v2/filtro/validar` y `POST /v2/filtro/invalidar-cache` (este último protegido por `FILTER_CACHE_SECRET`).
- [x] Reglas de permiso: `activo=0` ignorado; `activo=1` sin excepciones = bloqueada para todos; con excepciones = bloqueada salvo esos `user_id`.
- [x] Normalización que detecta variantes: acentos, separadores y leet speak.
- [x] Caché L1 (TTLCache) + L2 (Redis, TTL 12h) con building map anti-thundering-herd y degradación graceful a BD.
- [x] El MSA SMS deja de validar palabras prohibidas; el gateway orquesta la validación del unitario contra `/v2/filtro/validar` (ningún MSA llama a otro servicio).
- [x] Pruebas unitarias del servicio en verde.

---

## Validación

1. ### Configuración
`.env`: `FILTER_CACHE_SECRET=<valor>` y `DB_MASIVOS_SMS` apuntando a la BD con `filtro_sms` (esquema nuevo: `activo`) + `filtro_sms_excepciones`.

2. ### Pruebas unitarias del servicio
```bash
uv run pytest src/modules/process/test/unit/test_forbidden_words_service.py -v
```

3. ### Regresión completa
```bash
uv run pytest src/modules/process/test/unit/ -v
```

4. ### Validación manual del endpoint
```bash
curl -X POST http://localhost:9000/v2/filtro/validar \
  -H "Content-Type: application/json" \
  -d '{"texto":"compra droga aqui","user_id":9999}'
# Esperado: { "permitido": false, "palabras_bloqueadas": [{ "palabra": "droga", ... }] }
```

5. ### Invalidación de caché
```bash
curl -X POST http://localhost:9000/v2/filtro/invalidar-cache \
  -H "Content-Type: application/json" -d '{"secret":"<FILTER_CACHE_SECRET>"}'
# Esperado: { "invalidado": true }
```

6. ### Degradación
Con Redis caído, el servicio sigue validando vía BD (revisar logs `ForbiddenWords | cache miss total ...`).

---

## Consideraciones

- Variable de entorno nueva (en `.env.template` y `settings.py`): `FILTER_CACHE_SECRET`.
- Dependencias nuevas: `pyahocorasick`, `cachetools`.
- Tablas requeridas en `DB_MASIVOS_SMS`: `filtro_sms` (`id`, `termino`, `perfil`, `activo`, `created_at`) y `filtro_sms_excepciones` (`filtro_id`, `user_id`).
- El gateway debe enviar `infoUserValidSend.userId` para que apliquen las excepciones por usuario.
- Cambios complementarios fuera de este repo: gateway (`saem-v3`) orquesta `/filtro/validar` en el unitario; MSA SMS (`msa/sms`) deja de validar palabras prohibidas. Ambos en rama `black-list`.

---

# Planner Card

**Título:** [API Data Process](SMS) Feat: módulo de palabras prohibidas (Aho-Corasick + caché)

**Etiquetas:** FEAT | BACKEND

**Checklist:**
- [x] Servicio `ForbiddenWordsService` con Aho-Corasick + caché L1/L2 + building map
- [x] Paso `ValidateForbiddenWordsStep` en el pipeline SMS
- [x] Endpoints `/filtro/validar` e `/filtro/invalidar-cache`
- [x] Normalizador (acentos, separadores, leet speak)
- [x] Tests unitarios del servicio
- [x] Gateway orquesta el unitario; MSA deja de validar
- [ ] Crear merge request y asignar reviewer a Esteban
