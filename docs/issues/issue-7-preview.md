# GitLab Issue

**Título:** `[FEAT](PREVIEW) Implementar endpoint POST /first-rows para previsualización de archivos #7`

## Objetivo

- Agregar un endpoint de utilidad que permita previsualizar el contenido de un archivo CSV o XLSX antes de lanzar el procesamiento de una campaña
- El endpoint lee únicamente las primeras filas del archivo sin cargarlo completo en memoria, lo que lo hace eficiente para archivos de gran tamaño
- La detección de encoding y delimitador se realiza de forma automática, sin requerir parámetros adicionales del cliente
- El acceso está restringido al directorio autorizado del sistema; cualquier intento de acceder a rutas fuera de ese directorio es bloqueado
- Los errores de validación del payload retornan códigos de error tipados, consistentes con el patrón de respuesta del resto de la API
- El endpoint queda documentado en Swagger bajo el tag `Files`

## Resultado Esperado

- [x] El endpoint retorna las primeras filas del archivo para archivos CSV y XLSX válidos
- [x] El número máximo de filas retornadas es 6, independientemente del tamaño del archivo
- [x] El encoding y el delimitador se detectan automáticamente sin intervención del cliente
- [x] Las rutas fuera del directorio autorizado y los errores de validación del payload retornan el código de error tipado correcto
- [x] 17 tests pasan correctamente cubriendo detección de encoding, delimitador, lectura de primeras filas e integración del endpoint

---

## Validación

1. ### Detección automática de encoding (UTF-8 BOM, Latin-1/CP1252, ASCII)
```bash
uv run pytest src/modules/process/test/test_preview.py::TestDetectEncoding -v
```

2. ### Detección automática de delimitador (coma, punto y coma, tabulador)
```bash
uv run pytest src/modules/process/test/test_preview.py::TestDetectDelimiter -v
```

3. ### Lectura de primeras filas: CSV, XLSX, encodings alternativos, límite de filas y bloqueo de path traversal
```bash
uv run pytest src/modules/process/test/test_preview.py::TestGetFirstRows -v
```

4. ### Integración del endpoint: respuesta exitosa, archivo no encontrado, extensión inválida, campo vacío
```bash
uv run pytest src/modules/process/test/test_preview.py::test_endpoint_csv_ok src/modules/process/test/test_preview.py::test_endpoint_file_not_found src/modules/process/test/test_preview.py::test_endpoint_invalid_extension src/modules/process/test/test_preview.py::test_endpoint_empty_folder -v
```

5. ### Regresión completa
```bash
uv run pytest src/modules/process/test/ -v
```

---

# Planner Card

**Título:** `[API Data Process](Preview) Implementación endpoint POST /first-rows #7`

**Etiquetas:** `FEAT` `BACKEND` `TEST`

**Checklist:**
- [ ] Crear módulo de preview con lógica de lectura de primeras filas
- [ ] Implementar detección automática de encoding y delimitador
- [ ] Agregar endpoint POST /first-rows con validación de payload y protección de rutas
- [ ] Registrar el router en la aplicación y documentar en Swagger bajo tag Files
- [ ] Agregar tests del módulo y validar regresión completa
