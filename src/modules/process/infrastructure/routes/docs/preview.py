PREVIEW_OPENAPI_EXTRA = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["folder", "file"],
                    "properties": {
                        "folder":     {"type": "string",  "description": "Ruta absoluta al directorio que contiene el archivo."},
                        "file":       {"type": "string",  "description": "Nombre del archivo con extensión (CSV o XLSX)."},
                        "delimiter":  {"type": "string",  "description": "Delimitador de columnas para CSV. Vacío usa ';' por defecto.", "default": ""},
                        "useHeaders": {"type": "boolean", "description": "Si true, la primera fila se usa como encabezado.", "default": True},
                    },
                },
                "examples": {
                    "csv": {
                        "summary": "Archivo CSV con punto y coma",
                        "value": {
                            "folder":    "/home/dev_stefany/projects/Storage/data-process-prove",
                            "file":      "test_callb_standard_100.csv",
                            "delimiter": ";",
                            "useHeaders": True,
                        },
                    },
                    "xlsx": {
                        "summary": "Archivo XLSX",
                        "value": {
                            "folder":    "/home/dev_stefany/projects/Storage/data-process-prove",
                            "file":      "test_email_50_prueba.xlsx",
                            "useHeaders": True,
                        },
                    },
                },
            }
        },
    },
    "responses": {
        "200": {
            "description": "Vista previa generada correctamente.",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "success": {"type": "boolean"},
                            "headers": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Nombres de las columnas del archivo.",
                            },
                            "rows": {
                                "type": "array",
                                "items": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "description": "Filas de datos (máximo 6), cada una como lista de strings.",
                            },
                        },
                    },
                    "example": {
                        "success": True,
                        "headers": ["telefono", "nombre", "codigo"],
                        "rows": [
                            ["3001234567", "Ana", "A001"],
                            ["3009876543", "Bob", "B002"],
                        ],
                    },
                }
            },
        }
    },
}
