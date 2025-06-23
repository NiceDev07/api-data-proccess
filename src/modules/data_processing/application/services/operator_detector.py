import numpy as np
import dask.dataframe as dd

class OperatorDetector:
    def __init__(self, ranges: list[tuple[int, int, str]], default_operator: str = "N/A"):
        # Ordenar los rangos por 'inicio'
        sorted_ranges = sorted(ranges, key=lambda r: r[0])
        self.starts = np.array([r[0] for r in sorted_ranges], dtype=np.int64)
        self.ends = np.array([r[1] for r in sorted_ranges], dtype=np.int64)
        self.operators = np.array([r[2] for r in sorted_ranges], dtype=object)
        self.default_operator = default_operator

    def assign_operator(self, df: dd.DataFrame, phone_column: str) -> dd.DataFrame:
        def process_partition(partition):
            numbers = partition[phone_column].astype(np.int64).values
            idxs = np.searchsorted(self.starts, numbers, side="right") - 1
            result = np.full(len(numbers), self.default_operator, dtype=object)
            mask = (idxs >= 0) & (numbers <= self.ends[idxs])
            result[mask] = self.operators[idxs[mask]]
            partition["operador"] = result
            return partition

        meta = df._meta.assign(operador="object")
        return df.map_partitions(process_partition, meta=meta)
