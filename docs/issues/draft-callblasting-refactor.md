# GitLab Issue

**Título:** `[REFACTOR](CALLBLASTING) Elimina audioDuration, corrige doble margen de audio y mejora estimación de duración custom`

## Objetivo

- Eliminar `audioDuration` del DTO y del pipeline `CalculateDurationStandard`; `audioPath` queda como única fuente de duración para `standard`
- Corregir el doble margen de audio: el provider de ffprobe ya entrega `ceil(audio) + 5s`, el pipeline no debe sumarlo de nuevo
- Corregir la fórmula de duración de `CalculateDurationCustom` de `ceil(words/170*60) + 5` a `ceil(words/170*60 + 7)` con fallback para textos compactos sin espacios
- Hacer `content` opcional en `DataProcessingDTO` base; `SmsDataProcessingDTO` lo valida como requerido; `CallBlastingDataProcessingDTO` lo exige solo para `custom`
- Eliminar `OPERATION_MARGIN_SECS` de las constantes al no ser referenciado por ningún pipeline

## Resultado Esperado

- [X] Payload de `standard` sin `audioDuration` ni `content` es aceptado correctamente
- [X] Payload de `standard` sin `audioPath` retorna 422 con código `AUDIO_PATH_REQUIRED`
- [X] Payload de `custom` sin `content` retorna 422 con código `CONTENT_REQUIRED`
- [X] `CalculateDurationStandard` asigna a todos los registros el valor exacto retornado por el provider, sin margen adicional
- [X] `CalculateDurationCustom` produce 37s para 85 palabras y 67s para 170 palabras con la nueva fórmula
- [X] Texto compacto sin espacios (`"a" * 110`) usa fallback `len//5 = 22` palabras → 15s
- [X] La columna temporal `__cb_word_count__` no aparece en el DataFrame de salida

---

## Validación

1. ### Verifica que standard acepta audioPath y rechaza sin él

   ```bash
   uv run pytest src/modules/process/test/integration/test_callblasting_flow.py::test_cb_standard_happy_path src/modules/process/test/integration/test_callblasting_flow.py::test_cb_standard_audio_path_uses_provider -v
   ```

2. ### Verifica que el pipeline usa el valor del provider sin doble margen

   ```bash
   uv run pytest src/modules/process/test/unit/test_unit_pipelines.py::TestCalculateDurationStandard -v
   ```

3. ### Verifica la nueva fórmula de duración custom y el fallback compacto

   ```bash
   uv run pytest src/modules/process/test/unit/test_unit_pipelines.py::TestCalculateDurationCustom -v
   ```

4. ### Verifica créditos con duración corregida

   ```bash
   uv run pytest src/modules/process/test/integration/test_callblasting_flow.py::test_cb_standard_credits_calculation -v
   ```

5. ### Regresión completa

   ```bash
   uv run pytest src/modules/process/test/ -v
   ```

---

# Microsoft Planner Card

**Título:** `[REFACTOR](CALLBLASTING) Elimina audioDuration, corrige doble margen de audio y mejora estimación de duración custom`

**Descripción:**
Refactor del servicio Call Blasting: `audioDuration` eliminado del DTO dejando `audioPath` como única fuente para `standard`, se corrige el doble margen que inflaba los segundos, y se mejora la fórmula de duración de `custom` con fallback para textos compactos. `content` pasa a ser opcional en el DTO base.

**Lista de verificación:**

- [ ] Eliminar `audioDuration` del DTO y del pipeline `CalculateDurationStandard`
- [ ] `CalculateDurationStandard` usa valor del provider sin margen adicional
- [ ] `CalculateDurationCustom` con fórmula `ceil(words/170*60 + 7)` y fallback compacto
- [ ] `content = Optional` en `DataProcessingDTO`; validación en SMS y CB custom
- [ ] `OPERATION_MARGIN_SECS` eliminado de constantes
- [ ] Unit tests de duración actualizados (standard y custom)
- [ ] Tests de integración de CallBlasting sin `audio_duration`
- [ ] `uv run pytest` en verde
