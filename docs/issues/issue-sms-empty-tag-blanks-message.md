# GitLab Issue

**Título:** [FIX](SMS) Evitar que tags nulos en el template anulen el mensaje completo

## Objetivo

- Un `{tag}` con celda vacía propagaba `null` a toda la columna de mensaje en lugar de sustituir por cadena vacía.
- El comportamiento silenciaba envíos completos en BD sin error visible; reportado en producción con 6720/6756 filas afectadas.
- Reemplazar celdas nulas por cadena vacía antes de construir el mensaje, igual que se hace con otras columnas opcionales del pipeline.

## Resultado Esperado

- [x] Filas con `{tag}` vacío producen mensaje parcial en lugar de quedar nulas.
- [x] El campo `texto` en BD nunca llega vacío por un tag faltante en el CSV.
- [x] El nuevo caso de prueba y los tests previos pasan sin errores.

---

## Validación

1. ### Caso específico: null en tag no borra la fila
```bash
uv run pytest src/modules/process/test/test_sms_flow.py::TestCustomMessage::test_null_tag_value_does_not_blank_full_row -v
```

2. ### Regresión completa
```bash
uv run pytest src/modules/process/test/ -v
```

3. ### Validación manual
Subir CSV con columna `referencia` mayoritariamente vacía y template `"Su referencia es {referencia}."` → confirmar en BD que `texto` tiene el mensaje parcial y no está en blanco.

---

# Planner Card

**Título:** [API Data Process](SMS) Fix: tags nulos ya no anulan el mensaje completo

**Etiquetas:** FIX | BACKEND | ERROR

**Checklist:**
- [ ] Confirmar que la línea corregida reemplaza null por cadena vacía
- [ ] Ejecutar test específico del caso null-tag
- [ ] Ejecutar suite de regresión completa
- [ ] Validar campo `texto` en BD con CSV real de producción
- [ ] Cerrar issue y actualizar branch destino
