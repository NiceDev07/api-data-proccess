import polars as pl

class CostAssigner:
    def __init__(self, prefix_costs: list[tuple[str, float]], default_cost: float = 0.0):
        """
        prefix_costs: lista de tuplas (prefijo, costo)
        """
        # Ordenar por longitud de prefijo DESC para que el más específico gane
        self.prefix_costs = sorted(prefix_costs, key=lambda x: len(x[0]), reverse=True)
        self.default_cost = default_cost

    def assign_cost(self, df: pl.DataFrame, phone_column: str) -> pl.DataFrame:
        # Asegurar que los números sean string
        df = df.with_columns(pl.col(phone_column).cast(pl.Utf8))

        # Inicializar expresión de costo
        costo_expr = pl.lit(self.default_cost)

        # Construir la expresión condicional
        for prefix, cost in self.prefix_costs:
            print(f"Asignando costo {cost} para prefijo '{prefix}'")
            costo_expr = (
                pl.when(pl.col(phone_column).str.starts_with(prefix))
                .then(pl.lit(cost))
                .otherwise(costo_expr)
            )

        return df.with_columns(costo_expr.alias("__cost__"))
