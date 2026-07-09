import re

import polars as pl
from modules.process.app.normalizers.number import to_national_expr, to_national_str
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

        # El demográfico llega desde BD con prefijo de país (p. ej. 573027015524),
        # mientras que el archivo suele traer el número nacional (3027015524). La
        # concatenación del prefijo (ConcatPrefix) ocurre DESPUÉS de esta validación
        # y asume formato nacional, por eso comparamos en formato nacional: a ambos
        # lados les quitamos el prefijo si está presente. Persistir el número ya
        # normalizado además evita un doble prefijo si el archivo lo trae con el 57.
        prefix, full_len = self._national_prefix_info(payload)
        df = df.with_columns(
            to_national_expr(
                self._clean_expr(pl.col(number_col)), prefix, full_len
            ).alias(number_col)
        )
        target = to_national_str(self._clean_str(str(demographic)), prefix, full_len)

        mism = (pl.col(number_col).is_null()) | (pl.col(number_col) != pl.lit(target))
        bad = df.filter(mism).select(pl.col(number_col)).head(3).to_series().to_list()
        if bad:
            raise ValueError(
                "UNAUTHORIZED_RECORDS: The file contains records that do not match the authorized number for level 1 testing."
            )

        return df.lazy()

    @staticmethod
    def _national_prefix_info(payload: DataProcessingDTO) -> tuple[str, int]:
        """Prefijo del país y longitud total (prefijo + nacional) esperada.

        Reutiliza `RulesCountry.national_digits`, el mismo criterio que usa
        ConcatPrefix, para que ambos pasos concuerden en qué es un número nacional.
        """
        prefix = str(payload.rulesCountry.codeCountry)
        return prefix, len(prefix) + payload.rulesCountry.national_digits

    @staticmethod
    def _clean_expr(expr: pl.Expr) -> pl.Expr:
        """Normaliza para comparar, email-safe: quita espacios y, si el valor es
        puramente numérico con decimales (p. ej. '3001234567.0' que produce Excel),
        toma la parte entera. Los emails no matchean y pasan intactos."""
        e = expr.cast(pl.Utf8).str.strip_chars().str.replace_all(r"\s+", "")
        return (
            pl.when(e.str.contains(r"^\d+\.\d+$"))
            .then(e.str.replace(r"\.\d+$", ""))
            .otherwise(e)
        )

    @staticmethod
    def _clean_str(value: str) -> str:
        v = re.sub(r"\s+", "", value.strip())
        return re.sub(r"\.\d+$", "", v) if re.fullmatch(r"\d+\.\d+", v) else v
