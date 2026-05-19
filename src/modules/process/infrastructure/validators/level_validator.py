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
                    f"MAX_RECORDS_EXCEEDED: File exceeds the maximum allowed limit of "
                    f"{self.max_records_elevated:,} records. Received {df.height:,}."
                )
            return df.lazy()

        # ── Nivel 1: modo prueba — solo las N filas necesarias ───────────────
        demographic = payload.infoUserValidSend.demographic
        if not demographic:
            raise ValueError("DEMOGRAPHIC_REQUIRED: The user demographic value is not defined for level 1.")

        number_col = payload.configFile.nameColumnDemographic
        if number_col not in lf.collect_schema():
            raise ValueError(f"COLUMN_NOT_FOUND: The column '{number_col}' does not exist in the file.")

        df = lf.head(self.max_records).collect()
        df = df.with_columns(
            pl.col(number_col).cast(pl.Utf8).str.strip_chars().alias(number_col)
        )
        target = str(demographic).strip()

        mism = (pl.col(number_col).is_null()) | (pl.col(number_col) != pl.lit(target))
        bad = df.filter(mism).select(pl.col(number_col)).head(3).to_series().to_list()
        if bad:
            raise ValueError(
                "UNAUTHORIZED_RECORDS: The file contains records that do not match the authorized number for level 1 testing."
            )

        return df.lazy()
