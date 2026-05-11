import polars as pl
from modules.process.domain.interfaces.level_validator import IUserLevelValidator
from modules.process.domain.models.process_dto import DataProcessingDTO


class LevelValidator(IUserLevelValidator):
    def __init__(self, max_records: int = 10, max_records_elevated: int = 700_000):
        self.max_records = max_records
        self.max_records_elevated = max_records_elevated

    async def validate(self, lf: pl.LazyFrame, payload: DataProcessingDTO) -> pl.LazyFrame:
        # Los datos ya están en memoria (read_csv + .lazy()), collect() es instantáneo.
        # df.lazy() devuelve un LazyFrame en memoria para la cadena de pasos.
        if payload.infoUserValidSend.levelUser > 1:
            df = lf.collect()
            if df.height > self.max_records_elevated:
                raise ValueError(
                    f"El archivo supera el límite máximo permitido de "
                    f"{self.max_records_elevated:,} registros. "
                    f"Se recibieron {df.height:,}."
                )
            return df.lazy()

        # ── Nivel 1: modo prueba — solo las N filas necesarias ───────────────
        demographic = payload.infoUserValidSend.demographic
        if demographic is None:
            raise ValueError("Nivel 1: el valor demográfico del usuario no está definido.")

        number_col = payload.configFile.nameColumnDemographic
        if number_col not in lf.collect_schema():
            raise ValueError(f"Nivel 1: la columna '{number_col}' no existe en los datos.")

        df = lf.head(self.max_records).collect()
        df = df.with_columns(
            pl.col(number_col).cast(pl.Utf8).str.strip_chars().alias(number_col)
        )
        target = str(demographic).strip()

        mism = (pl.col(number_col).is_null()) | (pl.col(number_col) != pl.lit(target))
        bad = df.filter(mism).select(pl.col(number_col)).head(3).to_series().to_list()
        if bad:
            raise ValueError(
                "El archivo contiene registros que no corresponden al número autorizado para pruebas."
            )

        return df.lazy()
