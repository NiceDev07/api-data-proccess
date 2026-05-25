"""
Tests de error codes — valida que los errores de detalle que llegan al cliente
estén en inglés con su código identificable y no expongan rutas internas.
"""
import tempfile
from pathlib import Path

import polars as pl
import pytest

from modules.process.domain.constants.cols import Cols
from modules.process.domain.models.process_dto import DataProcessingDTO, InfoUserValidSend
from modules.process.infrastructure.validators.level_validator import LevelValidator

from modules.process.test.conftest import BASE_RULES_SMS, make_config_file, make_ctx

pytestmark = pytest.mark.anyio


# ─────────────────────────────────────────────────────────────────────────────
# LevelValidator — error codes
# ─────────────────────────────────────────────────────────────────────────────

def _lf(numbers: list[str]) -> pl.LazyFrame:
    return pl.DataFrame({"phone": numbers}).lazy()


def _ctx_level(level: int, demographic: str = "") -> DataProcessingDTO:
    return make_ctx(demographic="phone", rules=BASE_RULES_SMS).__class__(
        **{
            **make_ctx(demographic="phone", rules=BASE_RULES_SMS).model_dump(),
            "infoUserValidSend": InfoUserValidSend(levelUser=level, demographic=demographic),
        }
    )


async def test_max_records_exceeded_raises_with_code():
    validator = LevelValidator(max_records=10, max_records_elevated=5)
    lf = _lf(["3001234567"] * 6)
    ctx = _ctx_level(level=2)
    with pytest.raises(ValueError, match="MAX_RECORDS_EXCEEDED"):
        await validator.validate(lf, ctx)


async def test_max_records_exceeded_does_not_expose_internal_details():
    validator = LevelValidator(max_records=10, max_records_elevated=5)
    lf = _lf(["3001234567"] * 6)
    ctx = _ctx_level(level=2)
    with pytest.raises(ValueError) as exc_info:
        await validator.validate(lf, ctx)
    msg = str(exc_info.value)
    assert "5" in msg and "6" in msg


async def test_demographic_required_raises_with_code():
    validator = LevelValidator()
    lf = _lf(["3001234567"])
    ctx = _ctx_level(level=1, demographic="")
    with pytest.raises(ValueError, match="DEMOGRAPHIC_REQUIRED"):
        await validator.validate(lf, ctx)


async def test_column_not_found_raises_with_code():
    validator = LevelValidator()
    lf = pl.DataFrame({"otra_col": ["3001234567"]}).lazy()
    ctx = _ctx_level(level=1, demographic="3001234567")
    with pytest.raises(ValueError, match="COLUMN_NOT_FOUND"):
        await validator.validate(lf, ctx)


async def test_unauthorized_records_raises_with_code():
    validator = LevelValidator()
    lf = _lf(["3001234567", "3009999999"])
    ctx = _ctx_level(level=1, demographic="3001234567")
    with pytest.raises(ValueError, match="UNAUTHORIZED_RECORDS"):
        await validator.validate(lf, ctx)


# ─────────────────────────────────────────────────────────────────────────────
# ReaderFileFactory — FILE_NOT_FOUND para extensión desconocida
# ─────────────────────────────────────────────────────────────────────────────

def test_factory_unknown_extension_raises_file_not_found():
    from modules.process.app.files.factory import ReaderFileFactory
    factory = ReaderFileFactory()
    with pytest.raises(FileNotFoundError, match="FILE_NOT_FOUND"):
        factory.create("campana.txt")


def test_factory_no_extension_raises_file_not_found():
    from modules.process.app.files.factory import ReaderFileFactory
    factory = ReaderFileFactory()
    with pytest.raises(FileNotFoundError, match="FILE_NOT_FOUND"):
        factory.create("campana_sin_extension")


def test_factory_error_does_not_expose_path():
    from modules.process.app.files.factory import ReaderFileFactory
    factory = ReaderFileFactory()
    with pytest.raises(FileNotFoundError) as exc_info:
        factory.create("/ruta/interna/servidor/campana.txt")
    assert "/ruta/interna" not in str(exc_info.value)


# ─────────────────────────────────────────────────────────────────────────────
# CsvReader / XlsxReader — FILE_NOT_FOUND sin ruta expuesta
# ─────────────────────────────────────────────────────────────────────────────

async def test_csv_reader_file_not_found_raises_with_code():
    from modules.process.app.files.csv_reader import CsvReader
    from modules.process.domain.models.process_dto import BaseFileConfig
    config = BaseFileConfig(
        folder="/ruta/que/no/existe/campana.csv",
        file="campana.csv",
        delimiter=";",
        useHeaders=True,
        nameColumnDemographic="phone",
    )
    with pytest.raises(FileNotFoundError, match="FILE_NOT_FOUND"):
        await CsvReader().read(config)


async def test_csv_reader_error_does_not_expose_path():
    from modules.process.app.files.csv_reader import CsvReader
    from modules.process.domain.models.process_dto import BaseFileConfig
    config = BaseFileConfig(
        folder="/ruta/secreta/servidor/campana.csv",
        file="campana.csv",
        delimiter=";",
        useHeaders=True,
        nameColumnDemographic="phone",
    )
    with pytest.raises(FileNotFoundError) as exc_info:
        await CsvReader().read(config)
    assert "/ruta/secreta" not in str(exc_info.value)


async def test_xlsx_reader_file_not_found_raises_with_code():
    from modules.process.app.files.xlsx_reader import XlsxReader
    from modules.process.domain.models.process_dto import BaseFileConfig
    config = BaseFileConfig(
        folder="/ruta/que/no/existe/campana.xlsx",
        file="campana.xlsx",
        delimiter="",
        useHeaders=True,
        nameColumnDemographic="phone",
    )
    with pytest.raises(FileNotFoundError, match="FILE_NOT_FOUND"):
        await XlsxReader().read(config)
