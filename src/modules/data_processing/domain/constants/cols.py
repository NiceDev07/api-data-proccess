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
    is_special = "__IS_SPECIAL__" # ELIMINAR SOLO APLICA EN UN STEP EN PARTICULAR
    credit_base = "__CREDIT_BASE__" # ELIMINAR SOLO APLICA EN UN STEP EN PARTICULAR
    overhead = "__OVER_HEAD__" # ELIMINAR SOLO APLICA EN UN STEP EN PARTICULAR
    div = '__DIV__' # ELIMINAR SOLO APLICA EN UN STEP EN PARTICULAR
    is_ok = "__IS_OK__" # Indica los registros okey de la lista


