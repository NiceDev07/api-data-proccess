"""
Genera archivos de prueba para call blasting (standard y custom) en XLSX y/o CSV.

Uso:
    uv run python scripts/generate_test_xlsx_callb.py
    uv run python scripts/generate_test_xlsx_callb.py --rows 200000
    uv run python scripts/generate_test_xlsx_callb.py --rows 700000 --format csv
    uv run python scripts/generate_test_xlsx_callb.py --rows 100 --format both
    uv run python scripts/generate_test_xlsx_callb.py --rows 500 --dir /ruta/personalizada
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import polars as pl
from config.settings import settings

_PREFIXES = [
    "300", "301", "302", "303", "304", "305",
    "310", "311", "312", "313", "314", "315",
    "316", "317", "318", "319",
]

_NOMBRES = [
    "Carlos García", "María Rodríguez", "Juan Martínez", "Sofía López",
    "Andrés González", "Valentina Pérez", "Luis Sánchez", "Camila Ramírez",
    "Jorge Torres", "Daniela Flores",
]

_PRODUCTOS = ["cuenta de ahorros", "tarjeta de crédito", "préstamo personal", "seguro de vida"]
_SALDOS    = ["150000", "320500", "75000", "980000", "45200", "610000"]
_FECHAS    = ["15 de mayo de 2026", "20 de mayo de 2026", "25 de mayo de 2026", "30 de mayo de 2026"]

_CONTENT_STANDARD = (
    "Estimado {nombre}, le informamos que tiene una notificación pendiente en su cuenta. "
    "Para más información comuníquese con nuestra línea de atención disponible las 24 horas "
    "al número 018000123456. Recuerde que tiene 5 días hábiles para gestionar su solicitud. "
    "Atentamente, el equipo de servicios."
)

_CONTENT_CUSTOM = (
    "Estimado {nombre}, le informamos que su {producto} requiere atención. "
    "Comuníquese con nuestra línea de servicio al cliente disponible las 24 horas "
    "al número 018000123456 o visítenos en cualquiera de nuestras sucursales. "
    "Contamos con asesores especializados listos para ayudarle. Gracias por su preferencia."
)

_CONTENT_CUSTOM_LABELS = (
    "Estimado {nombre}, le informamos que su saldo actual es de {saldo} pesos "
    "con fecha de corte el {fecha}. Para mas informacion comuniquese con nosotros."
)


def _make_phones(n: int) -> list[str]:
    import random
    return [f"{random.choice(_PREFIXES)}{random.randint(1_000_000, 9_999_999)}" for _ in range(n)]


def make_standard_df(n: int) -> pl.DataFrame:
    return pl.DataFrame({
        "telefono": _make_phones(n),
        "nombre":   [_NOMBRES[i % len(_NOMBRES)] for i in range(n)],
    })


def make_custom_df(n: int) -> pl.DataFrame:
    return pl.DataFrame({
        "telefono": _make_phones(n),
        "nombre":   [_NOMBRES[i % len(_NOMBRES)] for i in range(n)],
        "producto": [_PRODUCTOS[i % len(_PRODUCTOS)] for i in range(n)],
    })


def make_custom_labels_df(n: int) -> pl.DataFrame:
    return pl.DataFrame({
        "telefono": _make_phones(n),
        "nombre":   [_NOMBRES[i % len(_NOMBRES)] for i in range(n)],
        "saldo":    [_SALDOS[i % len(_SALDOS)] for i in range(n)],
        "fecha":    [_FECHAS[i % len(_FECHAS)] for i in range(n)],
    })


def _write(df: pl.DataFrame, path: str, fmt: str) -> None:
    t0 = time.time()
    if fmt == "xlsx":
        df.write_excel(path)
    else:
        df.write_csv(path, separator=";")
    elapsed = time.time() - t0
    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f"  ✔  {os.path.basename(path)}  ({df.height:,} filas | {size_mb:.1f} MB | {elapsed:.1f}s)")


def _payload_standard(output_dir: str, fname: str, n: int, fmt: str) -> str:
    full_path = os.path.join(output_dir, fname)
    return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
POST /v2/processing/call_blasting — STANDARD  [{fmt.upper()}  {n:,} filas]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{{
  "content": "{_CONTENT_STANDARD}",
  "tariffId": 1,
  "campaignId": [99901],
  "subService": "standard",
  "audioDuration": 20,
  "useExclusionList": false,
  "configListExclusion": null,
  "configFile": {{
    "folder": "{full_path}",
    "file": "{fname}",
    "delimiter": ";",
    "useHeaders": true,
    "nameColumnDemographic": "telefono",
    "userIdentifier": false,
    "nameColumnIdentifier": "",
    "fileRecords": {n}
  }},
  "rulesCountry": {{
    "idCountry": 1,
    "codeCountry": 57,
    "useCharacterSpecial": false,
    "limitCharacter": 160,
    "limitCharacterSpecial": 70,
    "numberDigitsMobile": 10,
    "numberDigitsFixed": 7,
    "useShortName": false
  }},
  "infoUserValidSend": {{
    "levelUser": 2,
    "demographic": ""
  }}
}}

  ↳ Alternativa con audioPath (en lugar de audioDuration):
    "audioDuration": null,
    "audioPath": "/ruta/al/audio.wav"
"""


def _payload_custom(output_dir: str, fname: str, n: int, fmt: str) -> str:
    full_path = os.path.join(output_dir, fname)
    return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
POST /v2/processing/call_blasting — CUSTOM  [{fmt.upper()}  {n:,} filas]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{{
  "content": "{_CONTENT_CUSTOM}",
  "tariffId": 1,
  "campaignId": [99902],
  "subService": "custom",
  "audioDuration": null,
  "audioPath": null,
  "useExclusionList": false,
  "configListExclusion": null,
  "configFile": {{
    "folder": "{full_path}",
    "file": "{fname}",
    "delimiter": ";",
    "useHeaders": true,
    "nameColumnDemographic": "telefono",
    "userIdentifier": false,
    "nameColumnIdentifier": "",
    "fileRecords": {n}
  }},
  "rulesCountry": {{
    "idCountry": 1,
    "codeCountry": 57,
    "useCharacterSpecial": false,
    "limitCharacter": 160,
    "limitCharacterSpecial": 70,
    "numberDigitsMobile": 10,
    "numberDigitsFixed": 7,
    "useShortName": false
  }},
  "infoUserValidSend": {{
    "levelUser": 2,
    "demographic": ""
  }}
}}
"""


def _payload_custom_labels(output_dir: str, fname: str, n: int, fmt: str) -> str:
    full_path = os.path.join(output_dir, fname)
    return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
POST /v2/processing/call_blasting — CUSTOM + configLabels  [{fmt.upper()}  {n:,} filas]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{{
  "content": "{_CONTENT_CUSTOM_LABELS}",
  "tariffId": 1,
  "campaignId": [99903],
  "subService": "custom",
  "audioDuration": null,
  "audioPath": null,
  "configLabels": [
    {{"nameLabel": "nombre", "typeLabel": "N"}},
    {{"nameLabel": "saldo",  "typeLabel": "M"}},
    {{"nameLabel": "fecha",  "typeLabel": "S"}}
  ],
  "useExclusionList": false,
  "configListExclusion": null,
  "configFile": {{
    "folder": "{full_path}",
    "file": "{fname}",
    "delimiter": ";",
    "useHeaders": true,
    "nameColumnDemographic": "telefono",
    "userIdentifier": false,
    "nameColumnIdentifier": "",
    "fileRecords": {n}
  }},
  "rulesCountry": {{
    "idCountry": 1,
    "codeCountry": 57,
    "useCharacterSpecial": false,
    "limitCharacter": 160,
    "limitCharacterSpecial": 70,
    "numberDigitsMobile": 10,
    "numberDigitsFixed": 7,
    "useShortName": false
  }},
  "infoUserValidSend": {{
    "levelUser": 2,
    "demographic": ""
  }}
}}

  Resultado esperado en __MESSAGE__ por fila:
  "Estimado {{N:Carlos García}}, le informamos que su saldo actual es de {{M:150000}} pesos con fecha de corte el {{S:15 de mayo de 2026}}."
"""


def _payload_confirm(campaign_ids: list[int]) -> str:
    ids = ", ".join(str(c) for c in campaign_ids)
    return f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
POST /v2/confirm/call_blasting  (después del processing)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{{
  "campaignId": [{ids}],
  "codeGroup": null
}}
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera archivos de prueba para Call Blasting")
    parser.add_argument("--rows",   type=int, default=100,
                        help="Número de registros (default: 100). Ej: 200000, 300000, 700000")
    parser.add_argument("--format", choices=["xlsx", "csv", "both"], default="xlsx",
                        help="Formato de salida (default: xlsx)")
    parser.add_argument("--dir",    type=str, default=None,
                        help="Directorio de salida (default: REPOSITORY_FILES_DIR)")
    args = parser.parse_args()

    output_dir = args.dir or settings.REPOSITORY_FILES_DIR
    os.makedirs(output_dir, exist_ok=True)

    n       = args.rows
    formats = ["xlsx", "csv"] if args.format == "both" else [args.format]

    if n >= 200_000 and "xlsx" in formats:
        print(f"\n  ⚠  XLSX con {n:,} filas puede tardar varios minutos.")

    print(f"\n  Generando archivos en: {output_dir}\n")

    df_std = make_standard_df(n)
    df_cst = make_custom_df(n)

    df_lbl = make_custom_labels_df(n)

    for fmt in formats:
        ext = fmt
        fname_std = f"test_callb_standard_{n}.{ext}"
        fname_cst = f"test_callb_custom_{n}.{ext}"
        fname_lbl = f"test_callb_custom_labels_{n}.{ext}"
        _write(df_std, os.path.join(output_dir, fname_std), fmt)
        _write(df_cst, os.path.join(output_dir, fname_cst), fmt)
        _write(df_lbl, os.path.join(output_dir, fname_lbl), fmt)

        print(_payload_standard(output_dir, fname_std, n, fmt))
        print(_payload_custom(output_dir, fname_cst, n, fmt))
        print(_payload_custom_labels(output_dir, fname_lbl, n, fmt))

    print(_payload_confirm([99901, 99902, 99903]))


if __name__ == "__main__":
    main()
