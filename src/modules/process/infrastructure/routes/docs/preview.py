PREVIEW_OPENAPI_EXTRA = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["folder", "file"],
                    "properties": {
                        "folder": {"type": "string", "description": "Ruta absoluta al directorio que contiene el archivo."},
                        "file":   {"type": "string", "description": "Nombre del archivo con extensión (CSV o XLSX)."},
                    },
                },
                "examples": {
                    "csv": {
                        "summary": "Archivo CSV",
                        "value": {
                            "folder": "/home/dev_stefany/projects/Storage/data-process-prove",
                            "file": "test_callb_standard_100.csv",
                        },
                    },
                    "xlsx": {
                        "summary": "Archivo XLSX",
                        "value": {
                            "folder": "/home/dev_stefany/projects/Storage/data-process-prove",
                            "file": "test_email_50_prueba.xlsx",
                        },
                    },
                },
            }
        },
    }
}
