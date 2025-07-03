import polars as pl
import numpy as np

class OperatorDetector:
    def __init__(self, ranges: list[tuple[int, int, str]], default_operator: str = "N/A"):
        # Ordenar los rangos por inicio
        sorted_ranges = sorted(ranges, key=lambda r: r[0])
        self.starts = np.array([r[0] for r in sorted_ranges], dtype=np.int64)
        self.ends = np.array([r[1] for r in sorted_ranges], dtype=np.int64)
        self.operators = np.array([r[2] for r in sorted_ranges], dtype=object)
        self.default_operator = default_operator

    def assign_operator(self, df: pl.DataFrame, phone_column: str) -> pl.DataFrame:
        # Convertir a enteros si es necesario
        df = df.with_columns(pl.col(phone_column).cast(pl.Int64))

        # Obtener los números como array NumPy
        numbers = df[phone_column].to_numpy()

        # Buscar índice del rango en que caen
        idxs = np.searchsorted(self.starts, numbers, side="right") - 1
        valid = (idxs >= 0) & (numbers <= self.ends[idxs])
        assigned_ops = np.where(valid, self.operators[idxs], self.default_operator)

        # Insertar en el DataFrame de Polars
        return df.with_columns(pl.Series("__operator__", assigned_ops))
