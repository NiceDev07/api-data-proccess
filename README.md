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

  // Call Blasting standard
  "audioDuration": 45.0,                  // Segundos del audio (alternativa: audioPath)
  "audioPath": null
}
```

**Respuesta exitosa `200`:**  ver [Schemas de respuesta](#schemas-de-respuesta)

**Errores:**

| Código | Causa |
|--------|-------|
| `400`  | Validación del DTO o nivel inválido |
| `404`  | Archivo no encontrado en `configFile.folder` |
| `500`  | Error interno del pipeline |

---

### POST /confirm/{service}

Confirma el envío de una campaña ya procesada e inserta los datos en la base de datos upstream.

**Path param:** `service` — `sms` | `email` | `call_blasting`

**Request body:**

```jsonc
{
  "campaignId": [101, 102],
  "codeGroup": "GRP_2024_ABC"   // Opcional; prioridad sobre campaignId para buscar el Parquet
}
```

**Respuesta exitosa `200`:**

```jsonc
// SMS / Email
{ "inserted": 48320 }

// Call Blasting (pendiente de implementación)
{ "confirmed": [101, 102] }
```

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
9.  ValidateRegulations    Aplica hasta 3 regulaciones (ver sección Validaciones)
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
    Usa audioDuration o audioPath                              Sustituye {tags} en el script
    + margen operativo de 5 segundos                          │
    │                                                         ▼
    │                                                     7c2. CalculateDurationCustom
    │                                                          Estima duración por conteo de palabras
    │                                                          (170 palabras/min) + 5 seg de margen
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

Aplicadas en orden; un registro puede acumular múltiples violaciones.

| Código                      | Condición                                                            |
|-----------------------------|----------------------------------------------------------------------|
| `SHORTNAME_MISSING`         | `useShortName=true` y el mensaje no contiene el valor de `shortname` |
| `SPECIAL_CHAR_NOT_ALLOWED`  | `useCharacterSpecial=false` y el mensaje tiene caracteres Unicode    |
| `CHAR_LIMIT_EXCEEDED`       | Largo del mensaje supera `limitCharacter` (o `limitCharacterSpecial` si hay Unicode) |

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
| `SHORTNAME_MISSING`         | SMS             | Mensaje sin shortname requerido               |
| `SPECIAL_CHAR_NOT_ALLOWED`  | SMS             | Caracteres Unicode en mensaje sin permiso     |
| `CHAR_LIMIT_EXCEEDED`       | SMS             | Mensaje supera el límite de caracteres        |

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
