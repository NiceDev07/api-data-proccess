from typing import Final

class Cols:
    number_concat: Final[str] = "__number_concat__" # SAVE ->
    message: Final[str] = "__message__"
    number_operator: Final[str] = "__number_operator__"
    cost_operator: Final[str] = "__cost_operator__"
    credits: Final[str] = "__CREDITS__"
    pdu: Final[str] = "__PDU__"               # SMS: Protocol Data Units
    seconds: Final[str] = "__SECONDS__"       # Call Blasting: duración del audio en segundos
    initial: Final[str] = "__INITIAL__"       # Call Blasting: ciclos mínimos a facturar por llamada
    incremental: Final[str] = "__INCREMENTAL__"  # Call Blasting: segundos por ciclo de facturación
    identifier: Final[str] = "__IDENTIFIER__"
    cost: Final[str] = "__COST__"
    service: Final[str] = "__SERVICE__"
    length: Final[str] = "__LEN__"
    length_bytes: Final[str] = "__LENB__"
    is_special: Final[str] = "__IS_SPECIAL__"
    credit_base: Final[str] = "__CREDIT_BASE__"
    overhead: Final[str] = "__OVER_HEAD__"
    div: Final[str] = "__DIV__"
    is_ok: Final[str] = "__IS_OK__"
    error_code: Final[str] = "__ERROR_CODE__"
    email_domain: Final[str] = "__EMAIL_DOMAIN__"  # Email: dominio extraído (gmail.com, etc.)
    email: Final[str] = "__EMAIL__"                # Email: dirección normalizada
    subject: Final[str] = "__SUBJECT__"            # Email: asunto del correo
