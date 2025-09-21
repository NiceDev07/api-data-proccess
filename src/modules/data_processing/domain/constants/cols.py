from dataclasses import dataclass

@dataclass(frozen=True)
class Cols:
    number_concat = "__number_concat__"
    message = "__message__"
    number_operator = "__number_operator__"
    cost_operator = "__cost_operator__"
