# API Data Processing

Servicio async en FastAPI que procesa archivos de datos (CSV/XLSX) para campañas de comunicación masiva: **SMS**, **Email** y **Call Blasting**.

---

## Índice

- [Inicio rápido](#inicio-rápido)
- [Flujo general](#flujo-general)
- [Endpoints](#endpoints)
  - [POST /processing/{service}](#post-processingservice)
  - [POST /confirm/{service}](#post-confirmservice)
  - [GET /health](#get-health)
- [Servicios y sub-servicios](#servicios-y-sub-servicios)
- [Pipelines por servicio](#pipelines-por-servicio)
  - [SMS](#pipeline-sms)
  - [Email](#pipeline-email)
  - [Call Blasting](#pipeline-call-blasting)
- [Schemas de respuesta](#schemas-de-respuesta)
- [Validaciones](#validaciones)
- [Razones de exclusión](#razones-de-exclusión)
- [Flujo de confirmación](#flujo-de-confirmación)
- [Tests](#tests)
- [Variables de entorno](#variables-de-entorno)

---

## Inicio rápido

```bash
cp .env.template .env          # Configurar variables de entorno
uv sync                        # Instalar dependencias
uv run fastapi dev src/main.py --host 0.0.0.0  # Levantar servidor (dev)
```

**Base URL:** `http://host:8000/v2`

---

## Flujo general

```
Cliente
  │
  ▼
POST /v2/processing/{service}
  │
  ├─ Lectura del archivo (CSV / XLSX) → Polars DataFrame
  ├─ LevelValidator
  │     Level 1: máx 10 registros, filtra por número exacto
  │     Level >1: máx 700 000 registros
  │
  ├─ Pipeline específico del servicio (ver sección Pipelines)
  │     CleanData → Exclusiones → Operador → Costos → Contenido
  │     → Unidades (PDU / segundos) → Regulaciones → Créditos
  │
  ├─ SaveResults → archivo Parquet en resultados/Campaign/{service}/
  │
  └─ Respuesta JSON
       ├─ summaryGeneral  (totales: registros, créditos, excluidos)
       ├─ summaryGroup    (desglose por operador / dominio)
       └─ violations      (solo SMS: regulaciones incumplidas)

  │
  ▼  (opcional, tras revisión del cliente)
POST /v2/confirm/{service}
  │
  ├─ Lee el Parquet generado anteriormente
  ├─ Mapea columnas internas → columnas de BD
  └─ Bulk insert en base de datos upstream
```

---

## Endpoints

### POST /processing/{service}

Procesa un archivo de campaña y devuelve el resumen de costos.

**Path param:** `service` — `sms` | `email` | `call_blasting`

**Request body:**

```jsonc
{
  // Identificación de campaña
  "campaignId": [101, 102],         // IDs de campaña
  "codeGroup": "GRP_2024_ABC",      // Código de grupo (opcional; usado como clave de archivo)

  // Configuración del archivo de entrada
  "configFile": {
    "folder": "/data/uploads",
    "file": "contactos.csv",
    "delimiter": ",",
    "useHeaders": true,
    "nameColumnDemographic": "telefono",   // columna de teléfono / email
    "userIdentifier": false,
    "nameColumnIdentifier": "",
    "fileRecords": 50000
  },

  // Lista de exclusión (opcional)
  "useExclusionList": true,
  "configListExclusion": {
    "folder": "/data/exclusions",
    "file": "excluidos.csv",
    "delimiter": ",",
    "useHeaders": true,
    "nameColumnDemographic": "telefono",
    "paramIdentifier": "demographic"       // "demographic" | "identifier"
  },

  // Contenido del mensaje
  "content": "Hola {nombre}, tu código es {codigo}.",
  "subject": "Tu código de acceso",       // Solo email
  "shortname": "MIEMPRESA",               // Solo SMS

  // Servicio y sub-servicio
  "subService": "informative",            // Ver tabla de sub-servicios

  // Tarifa
  "tariffId": 5,

  // Reglas del país
  "rulesCountry": {
    "idCountry": 1,
    "codeCountry": 54,
    "useCharacterSpecial": false,
    "limitCharacter": 160,
    "limitCharacterSpecial": 70,
    "numberDigitsMobile": 10,
    "numberDigitsFixed": 10,
    "useShortName": true
  },

  // Nivel de usuario
  "infoUserValidSend": {
    "levelUser": 2,                        // 1=test | >1=producción
    "demographic": "5491112345678"         // Requerido si levelUser=1
  },

  // Call Blasting
  "audioPath": "/data/audio/mensaje.mp3", // standard: requerido; la duración se calcula con ffprobe
  "configLabels": []                      // custom: etiquetas TTS opcionales {N:valor}
}
```

**Respuesta exitosa `200`:**  ver [Schemas de respuesta](#schemas-de-respuesta)

**Errores:**

| Código | Causa |
|--------|-------|
| `400`  | `service` inválido en el path, validación de negocio del DTO (`ValueError`) o nivel inválido |
| `404`  | Archivo no encontrado en `configFile.folder` |
| `422`  | Campos del body faltantes o con tipo incorrecto (`MISSING_FIELD` / `INVALID_FIELD`) |
| `500`  | Error interno del pipeline |

Todos los errores devuelven `detail: { "code": "...", "message": "..." }`.

---

### POST /confirm/{service}

Confirma el envío de una campaña ya procesada e inserta los datos en la base de datos upstream.

**Path param:** `service` — `sms` | `email` | `call_blasting`

**Request body:**

```jsonc
{
  "campaignId": [101, 102],     // Requerido, al menos un elemento
  "codeGroup": "GRP_2024_ABC",  // Requerido; prioridad sobre campaignId para buscar el Parquet
  "userId": 12345               // Requerido; se devuelve tal cual en la respuesta
}
```

**Respuesta exitosa `200`:**

```jsonc
// SMS / Email / Call Blasting
{ "inserted": 48320, "userId": 12345 }

// Parquet sin registros válidos
{ "inserted": 0, "message": "No valid records to insert.", "userId": 12345 }
```

> Call Blasting solo inserta los registros con `__IS_OK__ = true`; SMS y Email insertan todos con su estado `P` / `X`.

---

### GET /health

```jsonc
{ "status": "ok" }
```

---

## Servicios y sub-servicios

| `service`       | `subService`   | Descripción                                  |
|-----------------|----------------|----------------------------------------------|
| `sms`           | `informative`  | SMS estándar                                 |
| `sms`           | `landing`      | SMS con link a landing page                  |
| `email`         | `standard`     | Email estándar                               |
| `call_blasting` | `standard`     | Audio pregrabado con duración fija           |
| `call_blasting` | `custom`       | Mensaje de texto convertido a voz (TTS)      |

---

## Pipelines por servicio

### Pipeline SMS

```
Archivo (CSV/XLSX)
    │
    ▼
1.  CleanData              Normaliza números: elimina nulos, cortos, convierte a Int64
    │
    ▼
2.  Exclution              Filtra registros que estén en la lista de exclusión
    │
    ▼
3.  AssignOperator         Asigna operador por rangos numéricos (NumerationService + Redis)
    │
    ▼
4.  ConcatPrefix           Agrega prefijo de país (ej. 54 para Argentina)
    │
    ▼
5.  AssignCost             Busca el costo por prefijo en tablas de tarifa (Redis → MySQL)
    │
    ▼
6.  CustomMessage          Sustituye {tags} en el mensaje con valores de columnas
    │
    ▼
7.  Landing                (subService=landing) Valida que el mensaje contenga URL
    │
    ▼
8.  CalculatePDU           PDU = ⌈chars / base⌉
                           base = 160 (estándar) | 70 (caracteres especiales)
                           multi-part: base - overhead (7 estándar | 3 especial)
    │
    ▼
9.  ValidateRegulations    Aplica 2 regulaciones activas (ver sección Validaciones)
    │
    ▼
10. CalculateCredits       créditos = PDU × costo_por_PDU
    │
    ▼
11. SaveResults            Guarda Parquet en resultados/Campaign/sms/
    │
    ▼
    Respuesta JSON
```

---

### Pipeline Email

```
Archivo (CSV/XLSX)
    │
    ▼
1.  CleanDataEmail         Normaliza emails: lowercase, trim; renombra a __EMAIL__
    │
    ▼
2.  ExclutionEmail         Filtra por comparación de string (no numérica)
    │
    ── Modo Lazy (evaluación diferida con Polars LazyFrame) ──────────────────
    │
    ▼
3.  ValidateEmail          Valida formato con regex RFC básico
    │
    ▼
4.  ExtractEmailDomain     Extrae dominio (ej. gmail.com); desconocidos → "others"
                           Dominios conocidos: gmail | hotmail | yahoo | outlook | icloud
    │
    ▼
5.  AssignCostEmail        Costo plano por email desde tarifa
    │
    ▼
6.  CustomMessage          Sustituye {tags} en el cuerpo del mensaje
    │
    ▼
7.  CustomSubject          Sustituye {tags} en el asunto del email
    │
    ▼
8.  CalculateCreditsEmail  créditos = costo (relación 1:1)
    │
    ▼
9.  SaveResults            Guarda Parquet en resultados/Campaign/email/
    │
    ▼
    Respuesta JSON
```

---

### Pipeline Call Blasting

```
Archivo (CSV/XLSX)
    │
    ▼
1.  CleanData              Normaliza teléfonos
    │
    ▼
2.  Exclution              Filtra lista de exclusión
    │
    ▼
3.  ValidatePhoneLength    Teléfono debe tener exactamente numberDigitsMobile o numberDigitsFixed dígitos
    │
    ▼
4.  AssignOperator         Asigna operador por rangos numéricos
    │
    ▼
5.  ConcatPrefix           Agrega prefijo de país
    │
    ▼
6.  AssignCostCallBlasting Costo por minuto + duración inicial + factor incremental

    │
    ├─────────── subService = standard ──────────────────────────────────────┐
    │                                                                         │
    ▼                                                                         ▼
7s. CalculateDurationStandard                              7c. CustomMessage
    Duración real del audio (audioPath, ffprobe)               Sustituye {tags} en el script
    + margen operativo de 5 segundos                          │
    │                                                         ▼
    │                                                     7c2. CalculateDurationCustom
    │                                                          Estima duración por conteo de palabras
    │                                                          (170 palabras/min) + 7 seg de margen
    │                                                         │
    └────────────────────────┬────────────────────────────────┘
                             │
                             ▼
8.  CalculateCreditsCallBlasting
    ciclos = ⌈segundos / incremental⌉
    créditos = ciclos × incremental × (costo / 60)
                             │
                             ▼
9.  SaveResults              Guarda Parquet en resultados/Campaign/call_blasting/
                             │
                             ▼
                         Respuesta JSON
```

---

## Schemas de respuesta

### SMS

```jsonc
{
  "success": true,
  "summaryGeneral": {
    "total_records": 48320,     // Registros válidos tras filtros
    "total_excluded": 1680,     // Registros excluidos por cualquier razón
    "total_pdu": 96640,         // PDUs totales
    "total_credits": 2415.50    // Créditos totales a cobrar
  },
  "summaryGroup": [
    {
      "operator": "Claro",
      "total": 25000,
      "pdu": 50000,
      "credits": 1250.00,
      "unit_value": 0.05        // créditos / registros
    },
    {
      "operator": "Movistar",
      "total": 23320,
      "pdu": 46640,
      "credits": 1165.50,
      "unit_value": 0.05
    }
  ],
  "violations": [
    {
      "code": "SHORTNAME_MISSING",
      "affected": 120,
      "description": "El mensaje no contiene el shortname requerido"
    }
  ]
}
```

### Email

```jsonc
{
  "success": true,
  "summaryGeneral": {
    "total_records": 31500,
    "total_excluded": 500,
    "total_credits": 315.00
  },
  "summaryGroup": [
    {
      "domain": "gmail.com",
      "total": 18000,
      "credits": 180.00,
      "unit_value": 0.01
    },
    {
      "domain": "others",
      "total": 13500,
      "credits": 135.00,
      "unit_value": 0.01
    }
  ]
}
```

### Call Blasting

```jsonc
{
  "success": true,
  "summaryGeneral": {
    "total_records": 10000,
    "total_excluded": 200,
    "total_seconds": 600000,
    "total_credits": 1200.00
  },
  "summaryGroup": [
    {
      "operator": "Claro",
      "total": 6000,
      "seconds": 360000,
      "credits": 720.00,
      "unit_value": 0.12
    }
  ]
}
```

---

## Validaciones

### Niveles de usuario

| `levelUser` | Máx. registros | Comportamiento |
|-------------|----------------|----------------|
| `1`         | 10             | Solo procesa registros donde `nameColumnDemographic == demographic` |
| `> 1`       | 700 000        | Procesa el archivo completo |

### Regulaciones SMS

Aplicadas en orden sobre los registros aún válidos; un registro conserva solo el primer código de exclusión que recibe.

| Código                      | Condición                                                            | Efecto |
|-----------------------------|----------------------------------------------------------------------|--------|
| `SHORTNAME_MISSING`         | `useShortName=true` y el mensaje no contiene el valor de `shortname` | Aborta la campaña completa (`SHORTNAME_REQUIRED_IN_ALL`) |
| `SPECIAL_CHAR_NOT_ALLOWED`  | `useCharacterSpecial=false` y el mensaje tiene caracteres Unicode    | Excluye el registro |
| `URL_REQUIRED`              | `subService=landing` y el mensaje no contiene una URL `http(s)://`   | Aborta la campaña completa (`URL_REQUIRED_IN_ALL`) |

> `CHAR_LIMIT_EXCEEDED` está **desactivada**: los mensajes largos no se rechazan, se cobran como multi-parte en `CalculatePDU`.

### Validación de teléfonos (Call Blasting)

El número de dígitos debe ser exactamente igual a `numberDigitsMobile` **o** `numberDigitsFixed`.  
Registros que no cumplan son excluidos con código `INVALID_NUMBER_LENGTH`.

### Validación de emails

Regex aplicado: `^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`  
Emails inválidos son excluidos con código `INVALID_EMAIL`.

---

## Razones de exclusión

Los registros excluidos quedan en el Parquet con `__IS_OK__ = false` y el código en `__ERROR_CODE__`.

| Código                      | Servicio        | Descripción                                   |
|-----------------------------|-----------------|-----------------------------------------------|
| `EXCLUSION_LIST`            | SMS / CB        | El número está en la lista de exclusión        |
| `NO_OPERATOR`               | SMS / CB        | El número no pertenece a ningún rango de operador |
| `INVALID_NUMBER_LENGTH`     | CB              | Dígitos del teléfono no coinciden con reglas del país |
| `INVALID_EMAIL`             | Email           | Formato de email inválido                      |
| `NO_COST`                   | CB              | Sin tarifa configurada para el prefijo del número (SMS no excluye: costo 0) |
| `SHORTNAME_MISSING`         | SMS             | Mensaje sin shortname requerido               |
| `SPECIAL_CHAR_NOT_ALLOWED`  | SMS             | Caracteres Unicode en mensaje sin permiso     |
| `URL_REQUIRED`              | SMS (landing)   | Mensaje sin URL en sub-servicio landing       |

---

## Flujo de confirmación

```
POST /v2/confirm/{service}
    │
    ├─ Busca Parquet por codeGroup (prioridad) o por campaignId
    │
    ▼
Mapeo de columnas internas → columnas de BD

┌─────────────────────────────┬───────────────────────┬───────────────────────┐
│ Columna interna (Parquet)   │ SMS → BD              │ Email → BD            │
├─────────────────────────────┼───────────────────────┼───────────────────────┤
│ __number_concat__           │ celular               │ —                     │
│ __EMAIL__                   │ —                     │ mail                  │
│ __message__                 │ texto                 │ body                  │
│ __SUBJECT__                 │ —                     │ subject               │
│ __number_operator__         │ operador              │ —                     │
│ __PDU__                     │ pdu                   │ —                     │
│ __CREDITS__                 │ credit                │ —                     │
│ __IDENTIFIER__              │ identificacion ("")   │ id_client ("")        │
│ __IS_OK__                   │ estado (P / X)        │ status (P / X)        │
└─────────────────────────────┴───────────────────────┴───────────────────────┘

    │
    ▼
Bulk insert por campaignId (paralelo)
    │
    ▼
{ "inserted": N }
```

> **Estado de registros:** `P` = Pendiente de envío (válido) | `X` = Excluido

---

## Tests

### Ejecución

```bash
uv run pytest                                   # Suite completa (~8 s, sin BD ni Redis)
uv run pytest src/modules/process/test/unit     # Solo unitarios de pipelines
uv run pytest src/modules/process/test/integration/test_sms_flow.py -v
uv run pytest -k "shortname" -v                 # Filtrar por nombre
uv run pytest -m realdb                         # Solo los tests contra BD real (ver abajo)
SAVE_TEST_RESULTS=1 uv run pytest               # Persistir Parquet/CSV/JSON de cada escenario
```

Configuración en `pyproject.toml` (`[tool.pytest.ini_options]`):

| Opción | Valor | Efecto |
|--------|-------|--------|
| `pythonpath` | `["src"]` | Los tests importan `modules.process...` sin instalar el paquete |
| `testpaths` | `["src"]` | Los scripts de benchmark `test_bulk_insert*.py` de la raíz no se recolectan |
| `addopts` | `-m 'not realdb'` | Los tests con BD real quedan deseleccionados por defecto |
| `markers` | `realdb` | Marcador registrado para los tests que necesitan MySQL real |

Los tests son async (`pytest.mark.anyio`); el `conftest.py` fija el backend en `asyncio`.
No requieren `.env`, MySQL ni Redis: toda la infraestructura se sustituye por mocks.

### Estructura

```
src/modules/process/test/
├── conftest.py                      Fixtures, fábricas de DTOs y mocks compartidos
├── unit/
│   ├── test_unit_pipelines.py           87 · un paso IPipeline por clase
│   └── test_forbidden_words_service.py  14 · ForbiddenWordsService + normalizador
└── integration/
    ├── test_sms_flow.py                 13 · SmsProcessor end-to-end sobre files/data.csv
    ├── test_sms_inline.py                5 · /processing/sms/unit (ProcessSmsInlineUseCase)
    ├── test_email_flow.py                9 · EmailProcessor (LazyFrame)
    ├── test_callblasting_flow.py        12 · CallBlastingProcessor standard / custom
    ├── test_preview.py                  21 · get_first_rows + POST /first-rows
    ├── test_send_email_test.py           7 · SendEmailTestUseCase (SMTP mockeado)
    ├── test_error_codes.py              16 · LevelValidator, readers, códigos de error
    ├── test_sms_confirm.py               6 · SmsConfirmRepository (MySQL, INSERT IGNORE)
    ├── test_email_confirm.py             6 · EmailConfirmRepository (MySQL, SP create_mail_table)
    ├── test_callblasting_confirm.py      5 · CallBlastingConfirmRepository (PostgreSQL)
    ├── test_confirm_cleanup.py           6 · BaseConfirmStrategy: búsqueda y borrado del Parquet
    └── test_integration_endpoints.py    16 · HTTP con httpx sobre create_app() (2 son realdb)

src/modules/data_processing/application/test/
└── test_required_columns.py              4 · módulo legacy (deshabilitado en main.py)
```

Total: **227 tests** (225 ejecutados por defecto, 2 `realdb`).

### Fixtures y helpers (`conftest.py`)

| Helper | Qué entrega |
|--------|-------------|
| `make_ctx(...)` | `DataProcessingDTO` base listo para pasar a un pipeline: `levelUser=2`, `tariffId=1`, `codeGroup="test_grp_001"`, sin lista de exclusión. Es la clase base, así que **no** ejecuta los validadores de `SmsDataProcessingDTO` / `CallBlastingDataProcessingDTO` |
| `make_config_file()` / `make_excl_config()` | `ConfigFile` / `ConfigListExclusion` apuntando a `files/data.csv` (delimitador `;`, columna `phone`) |
| `BASE_RULES_SMS` / `BASE_RULES_CHILE` | `RulesCountry` de Colombia (57, móvil 10 / fijo 7) y Chile (56, móvil 9 / fijo 8). Ambas con `useShortName=False` y `useCharacterSpecial=True`, es decir **regulaciones SMS desactivadas** salvo que el test las active |
| `AnalysisStorage(scenario)` | `IStorage` que escribe Parquet real en un directorio temporal y expone `last_df()` para releer el resultado del pipeline. Con `SAVE_TEST_RESULTS=1` escribe en `resultados/test_process/<scenario>/` junto con CSV y `summary.json` |
| `numeration_mock(starts, ends, operators)` | `NumerationService.get_ranges` → arrays NumPy ordenados. Default: CLARO `300xxxxxxx`, MOVISTAR `320xxxxxxx` |
| `cost_mock(costs)` | `CostService.get_costs` → `[(prefijo, costo, operador_tarifa)]`. Default `("57", 0.5, "COLOMBIA")` |
| `cb_cost_mock(rows)` | `CostService.get_costs_cb` → `[(prefijo, costo_por_minuto, operador, initial, incremental)]`. Default `("57", 60.0, "COLOMBIA", 30.0, 15.0)` |
| `email_cost_mock(cost)` | `CostService.get_email_cost` → costo plano. Default `0.02` |
| `exclusion_mock(numbers)` / `email_exclusion_mock(emails)` | Fuente de exclusión que devuelve un DataFrame de una columna (`phone` / `email`) |
| `duration_mock(seconds)` | Provider de duración de audio. Default `30.0` |
| `base_phone_df(numbers)` / `base_email_df(emails)` | DataFrames "post-CleanData" con `__IS_OK__=True` y `__ERROR_CODE__=None`, para arrancar un test en mitad de la cadena |

Los mocks `cost_mock` / `cb_cost_mock` usan `costs or default`, por lo que una lista vacía cae al default: para simular "sin tarifa" hay que construir el `MagicMock` a mano (como hace `test_no_cost_match_marks_excluded`).

### Qué cubre cada archivo

**Unitarios de pipelines** (`test_unit_pipelines.py`): cada clase `TestXxx` instancia un solo paso y verifica su contrato con DataFrames mínimos.

| Paso | Reglas verificadas |
|------|--------------------|
| `CleanData` | Elimina espacios, decimales de Excel, nulos y números con menos dígitos que `max(numberDigitsMobile, numberDigitsFixed)`; recorta el prefijo de país si el número lo trae |
| `ConcatPrefix` | `codeCountry × 10^dígitos + número` para Colombia y Chile |
| `CustomMessage` / `CustomSubject` | Sustitución de `{tags}` por columna y por fila; un valor nulo no vacía la fila; `SUBJECT_REQUIRED` si falta el asunto |
| `Landing` | Solo actúa con `subService=landing`; sin URL en cualquier fila válida aborta la campaña |
| `CalculatePDU` | 160 → 1 PDU, 161 → 2 (base 153 en multi-parte); Unicode 70 → 1, 71 → 2 (base 67); ASCII no se marca especial |
| `CalculateCredits` | `créditos = PDU × costo` |
| `AssignCost` (SMS) | Coincidencia por prefijo, gana el prefijo más largo, sin coincidencia → costo 0 y operador `""` |
| `AssignCostCallBlasting` | Asigna costo / operador / initial / incremental; sin coincidencia → `NO_COST` |
| `AssignCostEmail` | Costo plano para todos los dominios; `None` → `default_cost`; llamada con `(idCountry, tariffId)` |
| `AssignOperator` | Búsqueda binaria en rangos; fuera de rango → `NO_OPERATOR`; no pisa exclusiones previas |
| `Exclution` / `ExclutionEmail` | Lista desactivada o vacía → todo OK; coincidencia → `EXCLUSION_LIST` |
| `CleanDataEmail` / `ValidateEmail` / `ExtractEmailDomain` | Lowercase y trim, regex de formato, dominios conocidos vs `others`, `COLUMN_NOT_FOUND` |
| `ValidatePhoneLength` | Acepta exactamente `numberDigitsMobile` o `numberDigitsFixed`; otro largo → `INVALID_NUMBER_LENGTH` |
| `ShortNameRegulation` / `SpecialCharRegulation` / `ValidateRegulations` | Desactivadas por `rulesCountry`; shortname ausente aborta; Unicode sin permiso excluye; orden de aplicación |
| `CalculateDurationCustom` | `⌈palabras / 170 × 60⌉ + 7`; fallback para texto compacto (< 5 palabras y > 100 caracteres) |
| `CalculateDurationStandard` | Usa `audioPath` vía provider; mínimo 1 segundo; misma duración en todas las filas |
| `CalculateCreditsCallBlasting` | `ciclos = ⌈segundos / incremental⌉` si `segundos > initial`, si no `ciclos = initial`; `créditos = ciclos × incremental × costo / 60` |
| `CalculateCreditsEmail` | `créditos = costo`, redondeo a 3 decimales |

**`test_forbidden_words_service.py`**: bloqueo global vs usuarios autorizados, caché Redis (hit, miss, Redis caído, un solo acceso a BD ante concurrencia), límites de palabra (`amor` no bloquea `Marculo`), normalización de mayúsculas, tildes y separadores.

**Flujos de procesamiento** (usan `files/data.csv`, 2 números colombianos):

- `test_sms_flow.py`: camino feliz con conteos, PDU, créditos y `summaryGroup`; exclusión por lista y por `NO_OPERATOR`; `{tags}`; shortname all-or-nothing; validadores de `SmsDataProcessingDTO`; `NO_VALID_RECORDS`; costo 0; `unit_value = 0` cuando un grupo de tarifa queda 100 % excluido.
- `test_sms_inline.py`: resultado por número con operador y routing; fail-closed a `NO_OPERATOR` si el SP falla o el routing está incompleto; números con prefijo; `useFlash` → `saem2`.
- `test_email_flow.py`: `INVALID_EMAIL`, lista de exclusión, agrupación por dominio y `others`, columnas de auditoría en Parquet, todos excluidos → 0 créditos.
- `test_callblasting_flow.py`: standard (duración del provider, créditos, Parquet sin mensaje) y custom (mensaje y segundos por registro); exclusiones; sub-servicio inválido; todos excluidos.
- `test_preview.py`: CSV/XLSX con y sin encabezados, delimitadores, límite de 6 filas, `FILE_NOT_FOUND`, `ACCESS_DENIED` por path traversal, endpoint HTTP (200 / 404 / 422).
- `test_send_email_test.py`: envío personalizado por fila, destinatarios inválidos reportados sin envío, `NO_VALID_RECIPIENTS`, reutilización de la última fila, fallo SMTP individual.

**Confirmación y errores**:

- `test_*_confirm.py`: cada repositorio llama al SP / función de creación de tabla con el id correcto, inserta en la tabla dinámica (`campana_{id}`, `mail_{id}`, `details."campaign_{id}"`) con `INSERT IGNORE` / `ON CONFLICT DO NOTHING`, y devuelve 0 sin tocar la BD con un DataFrame vacío.
- `test_confirm_cleanup.py`: el Parquet se busca por `codeGroup` y luego por `campaignId` unidos con `-`; se elimina solo tras una confirmación exitosa; un fallo al borrar solo registra un warning.
- `test_error_codes.py`: `MAX_RECORDS_EXCEEDED`, `DEMOGRAPHIC_REQUIRED`, `COLUMN_NOT_FOUND`, `UNAUTHORIZED_RECORDS`, normalización de nivel 1 (prefijo de país, decimales de Excel, emails intactos), `FILE_NOT_FOUND` sin exponer rutas.
- `test_integration_endpoints.py`: `POST /processing/{sms|email}` con 2, 100 y 1 000 000 filas y `POST /confirm/{sms|email}` sobre la app real (`create_app()`) con dependencias mockeadas; servicio inválido → 400 con `{code, message}`; Parquet inexistente → 404 `FILE_NOT_FOUND`.

### Tests contra base de datos real (`realdb`)

`test_confirm_sms_real_db` y `test_confirm_email_real_db` insertan 1 000 000 de filas en las campañas `99999191` de `telefonos_campanas` y `mail_campaings`. Requieren `.env` con `DB_TELEFONOS_CAMPANAS` y `DB_EMAIL` apuntando a un entorno de pruebas y se ejecutan solo con `uv run pytest -m realdb`.

> Estado actual: ambos están desactualizados respecto a los repositorios (`SmsConfirmRepository` ya no acepta `engine=`, `EmailConfirmRepository` necesita un `AsyncEngine`, y el POST de email omite `userId`). Fallarán hasta que se actualicen; se dejan marcados para no perder el escenario de carga.

### Convenciones para nuevos tests

- Nombre `test_*.py`; unitarios de un paso en `unit/`, flujos y HTTP en `integration/`.
- Marcar con `pytestmark = pytest.mark.anyio` (o el decorador por función) y escribir `async def`.
- Construir el contexto con `make_ctx(...)` y las dependencias con las fábricas de mocks del `conftest`; no leer de MySQL ni Redis.
- Fijar valores exactos (`pdu == 1`, `credits == pytest.approx(0.5)`) en lugar de `> 0`: la fórmula es la regla de negocio que el test protege.
- Un test que necesite BD real lleva `@pytest.mark.realdb` y campañas con ids que no existan en producción.

---

## Variables de entorno

| Variable                 | Descripción                                         |
|--------------------------|-----------------------------------------------------|
| `DB_SAEM3`               | DSN MySQL — costos de tarifa                        |
| `DB_PORTABILIDAD`        | DSN MySQL — rangos de operadores                    |
| `DB_MASIVOS_SMS`         | DSN MySQL — SMS masivos                             |
| `DB_TELEFONOS_CAMPANAS`  | DSN MySQL — confirmación SMS                        |
| `DB_EMAIL`               | DSN MySQL — confirmación Email                      |
| `REDIS_URL`              | Conexión Redis (default: `redis://localhost:6379/0`)|
| `REPOSITORY_FILES_DIR`   | Ruta base de archivos de entrada                    |
| `OUTPUT_DIR`             | Directorio de salida Parquet (default: `resultados`)|
| `PREFIX_APP`             | Prefijo de rutas (default: `/v2`)                   |
| `HOST`                   | Host del servidor (default: `0.0.0.0`)              |
| `PORT`                   | Puerto del servidor (default: `8000`)               |
| `ENV`                    | Entorno: `dev` o `prod`                             |
