from dataclasses import dataclass

@dataclass(frozen=True)
class Cols:
    number_concat = "__number_concat__"
    message = "__message__"
    number_operator = "__number_operator__"
    cost_operator = "__cost_operator__"
    status = "__STATUS__"
    credits = "__CREDITS__"
    pdu = "__PDU__"
    identifier = "__IDENTIFIER__"
    cost = "__COST__"
    service = "__SERVICE__"
    length="__LEN__"
    length_bytes="__LENB__"
    is_special = "__IS_SPECIAL__"
    credit_base = "__CREDIT_BASE__"
    overhead = "__OVER_HEAD__"
    div = '__DIV__'


