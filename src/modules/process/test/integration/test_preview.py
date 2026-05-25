"""
Tests para el endpoint POST /v2/first-rows y la lógica interna de preview.

Cubre:
- Detección de encoding (utf_8_sig, cp1252, latin1)
- Detección de delimitador (coma, punto y coma, tab)
- Lectura de CSV y XLSX
- Manejo de archivo no encontrado
- Validación de path traversal
- Validación de extensión inválida vía Pydantic
"""
from pathlib import Path

import polars as pl
import pytest
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from modules.process.app.files.preview import (
    _detect_encoding,
    _detect_delimiter,
    get_first_rows,
)
from modules.process.infrastructure.routes.preview import router as preview_router


# ── app mínima para tests de integración ──────────────────────────────────────

def _make_app(base_dir: str) -> FastAPI:
    # Sobreescribe REPOSITORY_FILES_DIR con el directorio temporal del test
    import config.settings as _s
    _s.settings.repository_files_dir = base_dir
    app = FastAPI()
    app.include_router(preview_router, prefix="/v2")
    return app


# ── helper ────────────────────────────────────────────────────────────────────

def _write(tmp_path: Path, name: str, content: bytes) -> Path:
    p = tmp_path / name
    p.write_bytes(content)
    return p


# ── _detect_encoding ──────────────────────────────────────────────────────────

class TestDetectEncoding:
    def test_utf8_bom_detected(self, tmp_path):
        p = _write(tmp_path, "f.csv", "col\nvalor".encode("utf_8_sig"))
        assert _detect_encoding(str(p)) == "utf_8_sig"

    def test_latin1_or_cp1252_fallback(self, tmp_path):
        # \xe9 (é) es válido tanto en latin1 como en cp1252 — se acepta cualquiera de los dos
        p = _write(tmp_path, "f.csv", b"nombre\nJos\xe9")
        assert _detect_encoding(str(p)) in ("cp1252", "latin1")

    def test_ascii_detected_as_utf8sig(self, tmp_path):
        # ASCII es subconjunto de UTF-8 — lo captura el primer encoding de la lista
        p = _write(tmp_path, "f.csv", b"name,value\nalice,1")
        assert _detect_encoding(str(p)) == "utf_8_sig"


# ── _detect_delimiter ─────────────────────────────────────────────────────────

class TestDetectDelimiter:
    def _make(self, tmp_path: Path, content: str) -> str:
        p = tmp_path / "f.csv"
        p.write_bytes(content.encode("utf-8"))
        return str(p)

    def test_comma(self, tmp_path):
        assert _detect_delimiter(self._make(tmp_path, "a,b,c\n1,2,3"), "utf_8_sig") == ","

    def test_semicolon(self, tmp_path):
        assert _detect_delimiter(self._make(tmp_path, "a;b;c\n1;2;3"), "utf_8_sig") == ";"

    def test_tab(self, tmp_path):
        assert _detect_delimiter(self._make(tmp_path, "a\tb\tc\n1\t2\t3"), "utf_8_sig") == "\t"

    def test_comma_inside_quotes_not_counted(self, tmp_path):
        # Las comas dentro de comillas dobles no se cuentan como delimitador
        assert _detect_delimiter(self._make(tmp_path, '"a,b";c;d\n"x,y";1;2'), "utf_8_sig") == ";"


# ── get_first_rows ────────────────────────────────────────────────────────────

class TestGetFirstRows:
    @pytest.mark.anyio
    async def test_csv_returns_rows(self, tmp_path):
        _write(tmp_path, "data.csv", "nombre,edad\nAna,30\nBob,25\nCar,40".encode("utf-8"))
        result = await get_first_rows(str(tmp_path), "data.csv", str(tmp_path))
        assert result["success"] is True
        assert len(result["data"]) == 4  # encabezado + 3 filas

    @pytest.mark.anyio
    async def test_csv_max_6_rows(self, tmp_path):
        # 10 filas — debe retornar solo 6
        rows = "\n".join(f"fila{i}" for i in range(10))
        _write(tmp_path, "big.csv", rows.encode("utf-8"))
        result = await get_first_rows(str(tmp_path), "big.csv", str(tmp_path))
        assert len(result["data"]) == 6

    @pytest.mark.anyio
    async def test_csv_latin1_encoding(self, tmp_path):
        _write(tmp_path, "lat.csv", "nombre\nJosé\nNiño".encode("latin1"))
        result = await get_first_rows(str(tmp_path), "lat.csv", str(tmp_path))
        assert result["success"] is True
        assert len(result["data"]) >= 1

    @pytest.mark.anyio
    async def test_xlsx_returns_rows(self, tmp_path):
        df = pl.DataFrame({"nombre": ["Ana", "Bob"], "edad": [30, 25]})
        df.write_excel(str(tmp_path / "data.xlsx"))
        result = await get_first_rows(str(tmp_path), "data.xlsx", str(tmp_path))
        assert result["success"] is True
        assert len(result["data"]) >= 1

    @pytest.mark.anyio
    async def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="FILE_NOT_FOUND"):
            await get_first_rows(str(tmp_path), "noexiste.csv", str(tmp_path))

    @pytest.mark.anyio
    async def test_path_traversal_raises(self, tmp_path):
        with pytest.raises(PermissionError, match="ACCESS_DENIED"):
            await get_first_rows(str(tmp_path), "../../etc/hosts", str(tmp_path))


# ── endpoint integration ──────────────────────────────────────────────────────

@pytest.fixture
def csv_dir(tmp_path):
    _write(tmp_path, "test.csv", "nombre,edad\nAna,30\nBob,25".encode("utf-8"))
    return tmp_path


@pytest.mark.anyio
async def test_endpoint_csv_ok(csv_dir):
    app = _make_app(str(csv_dir))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/v2/first-rows", json={"folder": str(csv_dir), "file": "test.csv"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert isinstance(body["data"], list)


@pytest.mark.anyio
async def test_endpoint_file_not_found(csv_dir):
    app = _make_app(str(csv_dir))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/v2/first-rows", json={"folder": str(csv_dir), "file": "noexiste.csv"})
    assert resp.status_code == 404
    assert "FILE_NOT_FOUND" in resp.json()["detail"]


@pytest.mark.anyio
async def test_endpoint_invalid_extension(csv_dir):
    app = _make_app(str(csv_dir))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/v2/first-rows", json={"folder": str(csv_dir), "file": "script.py"})
    assert resp.status_code == 422
    assert "INVALID_EXTENSION" in str(resp.json())


@pytest.mark.anyio
async def test_endpoint_empty_folder(csv_dir):
    app = _make_app(str(csv_dir))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/v2/first-rows", json={"folder": "", "file": "test.csv"})
    assert resp.status_code == 422
    assert "FIELD_REQUIRED" in str(resp.json())
