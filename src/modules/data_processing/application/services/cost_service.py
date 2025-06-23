from modules.data_processing.domain.value_objects.tel_cost_info import TelcoCostInfo
import dask.dataframe as dd
import pandas as pd
import numpy as np


class CostCalculatorService:
    def __init__(self, cost_info: TelcoCostInfo):
        self.prefixes = cost_info.prefixes
        self.costs = cost_info.costs
        self.lengths = cost_info.lengths

    def assign_costs(self, df: dd.DataFrame, number_column: str, new_col: str = "__cost__") -> dd.DataFrame:
        prefixes = self.prefixes
        costs = self.costs

        def match_prefix_and_assign(series):
            numbers = series.astype(str).values
            result = np.full_like(numbers, np.nan, dtype=float)
            for prefix, cost in zip(prefixes, costs):
                mask = np.char.startswith(numbers, prefix)
                result[mask] = cost  # Overwrites with more specific prefix
            return pd.Series(result, index=series.index)

        return df.map_partitions(
            lambda pdf: pdf.assign(**{new_col: match_prefix_and_assign(pdf[number_column])}),
            meta=df.head(0).assign(**{new_col: np.float64()})  # <--- explícito
        )
