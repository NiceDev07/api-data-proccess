from modules.data_processing.domain.interfaces.pipelines import IPipeline
import polars as pl
from ..schemas.preload_camp_schema import DataProcessingDTO
from modules.data_processing.domain.constants.cols import Cols

import polars as pl

import polars as pl

class ValidateLevel(IPipeline):
    def __init__(self, cols: Cols = Cols()):
        self.cols = cols

    def execute(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
        level = getattr(ctx.infoUserValidSend, "levelUser", None)
        if level is None:
            raise ValueError("El nivel de validación no ha sido definido.")

        # Solo aplica a nivel 1
        if level > 1:
            return df

        # Valor demográfico del usuario (número/correo) y columna del CSV
        demographic_user = ctx.infoUserValidSend.demographic
        if demographic_user is None:
            raise ValueError("Nivel 1: el valor demográfico del usuario no está definido.")

        number_col = ctx.configFile.nameColumnDemographic

        # 1) limitar a los primeros 10
        df10 = df.head(10)

        # 2) normalizar: cast a texto y strip en el DF y en el target
        df10 = df10.with_columns(
            pl.col(number_col).cast(pl.Utf8).str.strip_chars().alias(number_col)
        )
        target = str(demographic_user).strip()

        # 3) discrepancias (incluye nulos como mismatch)
        mism = (pl.col(number_col).is_null()) | (pl.col(number_col) != pl.lit(target))

        # 4) si hay alguna discrepancia, error con ejemplos
        bad = df10.filter(mism).select(pl.col(number_col)).head(3).to_series().to_list()
        if bad:
            raise ValueError(
                f"Nivel 1: todos los registros deben tener {number_col} = '{target}'. "
                f"Se encontraron valores distintos (ejemplos): {bad}"
            )

        return df10

