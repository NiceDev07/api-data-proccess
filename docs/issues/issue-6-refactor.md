# GitLab Issue

**Título:** `[REFACTOR](PROCESS) Limpieza y mejoras de calidad de código #6`

## Objetivo

- Corregir defectos técnicos de arquitectura, pipelines y configuración identificados durante la revisión de la rama `6-feat-dev`
- Separar responsabilidades en la capa de rutas y centralizar la documentación Swagger
- Estandarizar la configuración de la aplicación para que falle de forma clara al arrancar si faltan variables de entorno requeridas

## Resultado Esperado

- [x] La capa de procesamiento está correctamente desacoplada de la infraestructura, respetando la separación de capas
- [x] El servidor no se bloquea durante la asignación de operadores en procesamientos masivos
- [x] Los registros previamente excluidos conservan el motivo de exclusión correcto durante todo el proceso
- [x] Las rutas, dependencias y documentación de la API están organizadas en archivos separados por responsabilidad
- [x] La aplicación detecta al arrancar si falta alguna variable de configuración y lo reporta con un mensaje claro
- [x] Se eliminaron elementos sin uso, se corrigieron configuraciones de pruebas y se agregaron tests faltantes

---

## Validación

1. ### Correcciones de pipeline y exclusión
```bash
uv run pytest src/modules/process/test/test_unit_pipelines.py -v
```

2. ### Tests de CallBlastingConfirmRepository
```bash
uv run pytest src/modules/process/test/test_callblasting_confirm.py -v
```

3. ### Regresión completa
```bash
uv run pytest src/modules/process/test/ -v
```

---

# Planner Card

**Título:** `[API Data Process](Refactor) Limpieza y mejoras de calidad de código #6`

**Etiquetas:** `REFACTOR` `BACKEND` `FIX` `TEST`

**Checklist:**
- [ ] Corregir dependencia de infraestructura en capa de aplicación (confirm base → IStorage)
- [ ] Corregir bloqueo del event loop y sobreescritura de exclusiones en AssignOperator
- [ ] Separar dependencias FastAPI y documentación Swagger de la capa de rutas
- [ ] Migrar Settings a Pydantic con variables de BD requeridas
- [ ] Mover deps de arranque a carpeta startup/
- [ ] Aplicar correcciones menores y validar regresión completa
