from modules.process.domain.interfaces.level_validator import IUserLevelValidator  
from modules.process.domain.models.process_dto import DataProcessingDTO
import polars as pl

class LevelValidator(IUserLevelValidator):
    async def validate(self, df: pl.DataFrame, payload: DataProcessingDTO) -> pl.DataFrame:
        if payload.infoUserValidSend.levelUser > 1:
            return df
        
        demographic = payload.infoUserValidSend.demographic
        if demographic is None:
            raise ValueError("Nivel 1: el valor demográfico del usuario no está definido.")

        number_col = payload.configFile.nameColumnDemographic

        # 1) limitar a los primeros 10
        df = df.head(10)

        # 2) normalizar: cast a texto y strip en el DF y en el target
        df = df.with_columns(
            pl.col(number_col).cast(pl.Utf8).str.strip_chars().alias(number_col)
        )
        target = str(demographic).strip()

        # 3) discrepancias (incluye nulos como mismatch)
        mism = (pl.col(number_col).is_null()) | (pl.col(number_col) != pl.lit(target))

        # 4) si hay alguna discrepancia, error con ejemplos
        bad = df.filter(mism).select(pl.col(number_col)).head(3).to_series().to_list()
        if bad:
            raise ValueError(
                f"Nivel 1: todos los registros deben tener {number_col} = '{target}'. "
                f"Se encontraron valores distintos (ejemplos): {bad}"
            )

        return df

