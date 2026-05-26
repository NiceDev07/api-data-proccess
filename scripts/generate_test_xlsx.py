"""
Genera archivos XLSX de prueba para SMS, Email y Call Blasting e imprime
el body JSON completo listo para pegar en Postman.

Uso:
    uv run python scripts/generate_test_xlsx.py --service sms
    uv run python scripts/generate_test_xlsx.py --service email --rows 500
    uv run python scripts/generate_test_xlsx.py --service callblasting --no-headers
    uv run python scripts/generate_test_xlsx.py --service sms --rows 1000 --out mi_archivo.xlsx
"""

import argparse
import json
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import polars as pl
from config.settings import settings

_PREFIXES = [
    "300", "301", "302", "303", "304", "305",
    "310", "311", "312", "313", "314", "315",
    "316", "317", "318", "319", "320", "321",
]

_NOMBRES = [
    "Carlos García", "María Rodríguez", "Juan Martínez", "Sofía López",
    "Andrés González", "Valentina Pérez", "Luis Sánchez", "Camila Ramírez",
    "Jorge Torres", "Daniela Flores", "Miguel Rivera", "Isabella Gómez",
    "Alejandro Díaz", "Natalia Cruz", "Felipe Morales", "Gabriela Reyes",
]

_DOMINIOS = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "empresa.co"]

_RULES_COUNTRY = {
    "idCountry": 81,
    "codeCountry": 57,
    "useCharacterSpecial": False,
    "limitCharacter": 160,
    "limitCharacterSpecial": 70,
    "numberDigitsMobile": 10,
    "numberDigitsFixed": 7,
    "useShortName": False,
}


def _phone() -> str:
    return f"{random.choice(_PREFIXES)}{random.randint(1_000_000, 9_999_999)}"


def _email_addr(nombre: str) -> str:
    slug = nombre.lower().split()[0]
    return f"{slug}{random.randint(10, 99)}@{random.choice(_DOMINIOS)}"


def _make_sms(n: int) -> pl.DataFrame:
    nombres = [_NOMBRES[i % len(_NOMBRES)] for i in range(n)]
    return pl.DataFrame({"number": [_phone() for _ in range(n)], "nombre": nombres})


def _make_email(n: int) -> pl.DataFrame:
    nombres = [_NOMBRES[i % len(_NOMBRES)] for i in range(n)]
    return pl.DataFrame({"email": [_email_addr(n) for n in nombres], "nombre": nombres})


def _make_callblasting(n: int) -> pl.DataFrame:
    nombres = [_NOMBRES[i % len(_NOMBRES)] for i in range(n)]
    return pl.DataFrame({
        "number": [_phone() for _ in range(n)],
        "nombre": nombres,
        "saldo":  [f"${random.randint(1000, 999999):,}" for _ in range(n)],
    })


def _build_body(service: str, output_path: str, filename: str, n: int,
                use_headers: bool, col_name: str) -> dict:
    config_file = {
        "folder":                output_path,
        "file":                  filename,
        "delimiter":             ";",
        "useHeaders":            use_headers,
        "nameColumnDemographic": col_name,
        "userIdentifier":        False,
        "nameColumnIdentifier":  "",
        "fileRecords":           n,
    }

    if service == "sms":
        return {
            "content":          "Hola {nombre}, este es un mensaje de prueba.",
            "tariffId":         1,
            "campaignId":       [100010],
            "codeGroup":        "ABCD4123",
            "subService":       "informative",
            "useExclusionList": False,
            "configFile":       config_file,
            "rulesCountry":     _RULES_COUNTRY,
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        }

    if service == "email":
        return {
            "content":          "<p>Estimado {nombre}, su solicitud ha sido procesada.</p>",
            "subject":          "Notificación de prueba",
            "tariffId":         1,
            "campaignId":       [100010],
            "codeGroup":        "ABCD4123",
            "subService":       "standard",
            "useExclusionList": False,
            "configFile":       config_file,
            "rulesCountry":     _RULES_COUNTRY,
            "infoUserValidSend": {"levelUser": 2, "demographic": ""},
        }

    return {
        "content":          "Estimado {nombre}, su saldo es {saldo}.",
        "tariffId":         1,
        "campaignId":       [100010],
        "codeGroup":        "ABCD4123",
        "subService":       "standard",
        "audioDuration":    20,
        "audioPath":        None,
        "useExclusionList": False,
        "configFile":       config_file,
        "rulesCountry":     _RULES_COUNTRY,
        "infoUserValidSend": {"levelUser": 2, "demographic": ""},
    }


_BUILDERS = {
    "sms":          (_make_sms,          "number", "sms"),
    "email":        (_make_email,        "email",  "email"),
    "callblasting": (_make_callblasting, "number", "call_blasting"),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera XLSX de prueba para processing")
    parser.add_argument("--service",    choices=["sms", "email", "callblasting"], default="sms")
    parser.add_argument("--rows",       type=int,  default=100)
    parser.add_argument("--no-headers", action="store_true")
    parser.add_argument("--out",        type=str,  default=None)
    parser.add_argument("--dir",        type=str,  default=None)
    args = parser.parse_args()

    builder, demographic_col, service_url = _BUILDERS[args.service]
    use_headers = not args.no_headers
    suffix      = "no_headers" if not use_headers else "headers"
    filename    = args.out or f"test_{args.service}_{args.rows}_{suffix}.xlsx"
    output_dir  = args.dir or settings.REPOSITORY_FILES_DIR
    output_path = os.path.join(output_dir, filename)

    os.makedirs(output_dir, exist_ok=True)

    df = builder(args.rows)

    if use_headers:
        df.write_excel(output_path)
        col_name = demographic_col
    else:
        df.write_excel(output_path, include_header=False)
        col_name = "column_1"

    body = _build_body(args.service, output_path, filename, df.height, use_headers, col_name)

    print(f"\n  Archivo   : {output_path}")
    print(f"  Servicio  : {args.service}")
    print(f"  Registros : {df.height:,}")
    print(f"  Encabezado: {'sí' if use_headers else 'no'}")
    print(f"\n  POST /v2/processing/{service_url}\n")
    print(json.dumps(body, indent=2, ensure_ascii=False))
    print()


if __name__ == "__main__":
    main()
