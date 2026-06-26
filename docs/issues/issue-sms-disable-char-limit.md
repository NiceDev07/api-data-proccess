# GitLab Issue

**Título:** [FIX](SMS) Desactivar validación de límite de caracteres para replicar comportamiento del flujo activo

## Objetivo
- La validación de caracteres máximos rechazaba mensajes largos, pero el flujo activo del sistema previo los cobra como multi-parte sin techo.
- Desactivar esa validación para que mensajes largos pasen el pipeline y se cobren en PDU adicionales.
- Conservar el código comentado para facilitar una eventual reactivación controlada.

## Resultado Esperado
- [x] Mensajes que superen el límite de caracteres pasan el pipeline y se cobran como multi-parte sin ser rechazados.
- [x] Los tests del nuevo comportamiento pasan y la regresión completa no reporta errores inesperados.

---

## Validación

1. ### Nuevo comportamiento: mensaje largo cobra PDU multi-parte
```bash
uv run pytest src/modules/process/test/integration/test_sms_flow.py::test_sms_flow_long_message_uses_multipart_pdu -v
```

2. ### Regresión completa
```bash
uv run pytest src/modules/process/test/ -v
```

---

# Planner Card

**Título:** [API Data Process](SMS) Desactivar rechazo de mensajes largos en pipeline SMS

**Etiquetas:** FIX | BACKEND | TEST

**Checklist:**
- [ ] Desactivar validación que rechazaba mensajes por exceso de caracteres
- [ ] Verificar que mensajes largos se cobran como PDU multi-parte
- [ ] Conservar el código desactivado comentado para futura reactivación
- [ ] Reemplazar test de rechazo por test de comportamiento multi-parte
- [ ] Ejecutar regresión completa sin errores inesperados
