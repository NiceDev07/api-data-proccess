"""
Polars performance benchmark — Email pipeline transforms + CalculatePDU.

Compara:
  A) Email pure-transform chain: 6 pasos eager (step-by-step) vs lazy (collect al final)
  B) CalculatePDU: 5 with_columns separados vs 1 expresión fused

Usage:
    uv run python benchmark_polars.py
"""
import time
import statistics

import polars as pl

# ------------------------------------------------------------------
# Column names (same as Cols constants)
# ------------------------------------------------------------------
EMAIL         = "__EMAIL__"
IS_OK         = "__IS_OK__"
ERROR_CODE    = "__ERROR_CODE__"
EMAIL_DOMAIN  = "__EMAIL_DOMAIN__"
COST          = "__COST__"
MESSAGE       = "__message__"
SUBJECT       = "__SUBJECT__"
CREDITS       = "__CREDITS__"
LENGTH        = "__LEN__"
LENGTH_BYTES  = "__LENB__"
IS_SPECIAL    = "__IS_SPECIAL__"
CREDIT_BASE   = "__CREDIT_BASE__"
OVERHEAD      = "__OVER_HEAD__"
DIV           = "__DIV__"
PDU           = "__PDU__"

EMAIL_RE  = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
COST_VAL  = 0.025
CONTENT   = "Hola {__EMAIL__}, tu código es 1234"
SUBJ_TMPL = "Promoción para {__EMAIL__}"

PDU_STD, OVH_STD = 160, 7
PDU_SPC, OVH_SPC = 70,  3

RUNS       = 5
ROW_COUNTS = [10_000, 100_000, 500_000, 1_000_000]

# ------------------------------------------------------------------
# Synthetic data builders
# ------------------------------------------------------------------

def make_email_df(n: int) -> pl.DataFrame:
    base_emails = [
        "user@gmail.com", "otro@hotmail.com", "bad-email", "test@yahoo.com",
        "invalid", "real@outlook.com",
    ]
    emails = [base_emails[i % len(base_emails)] for i in range(n)]
    return pl.DataFrame({
        EMAIL:      emails,
        IS_OK:      [True] * n,
        ERROR_CODE: [None] * n,
    }).with_columns(pl.col(ERROR_CODE).cast(pl.Utf8))


def make_sms_df(n: int) -> pl.DataFrame:
    msgs = [
        "Hola mundo, este es un mensaje estándar.",
        "Mensáje con caractéres espéciáles üñ 😊",
        "Short",
        "A" * 200,
    ]
    return pl.DataFrame({
        MESSAGE: [msgs[i % len(msgs)] for i in range(n)],
    })


# ------------------------------------------------------------------
# Email chain — EAGER (step by step, current behavior)
# ------------------------------------------------------------------

def email_chain_eager(df: pl.DataFrame) -> pl.DataFrame:
    # ValidateEmail
    is_valid    = pl.col(EMAIL).str.contains(EMAIL_RE)
    to_invalid  = pl.col(IS_OK) & ~is_valid
    df = df.with_columns(
        pl.when(to_invalid).then(pl.lit(False)).otherwise(pl.col(IS_OK)).alias(IS_OK),
        pl.when(to_invalid).then(pl.lit("INVALID_EMAIL")).otherwise(pl.col(ERROR_CODE)).alias(ERROR_CODE),
    )

    # ExtractEmailDomain
    df = df.with_columns(
        pl.col(EMAIL).str.splitn("@", 2).struct.field("field_1").alias(EMAIL_DOMAIN)
    )

    # AssignCostEmail
    df = df.with_columns(pl.lit(COST_VAL).alias(COST))

    # CustomMessage
    df = df.with_columns(
        pl.format("Hola {}, tu código es 1234", pl.col(EMAIL).cast(pl.Utf8)).alias(MESSAGE)
    )

    # CustomSubject
    df = df.with_columns(
        pl.format("Promoción para {}", pl.col(EMAIL).cast(pl.Utf8)).alias(SUBJECT)
    )

    # CalculateCreditsEmail
    df = df.with_columns(
        pl.col(COST).cast(pl.Float64).round(3).alias(CREDITS)
    )

    return df


# ------------------------------------------------------------------
# Email chain — LAZY (single plan, collect at the end)
# ------------------------------------------------------------------

def email_chain_lazy(df: pl.DataFrame) -> pl.DataFrame:
    is_valid   = pl.col(EMAIL).str.contains(EMAIL_RE)
    to_invalid = pl.col(IS_OK) & ~is_valid

    return (
        df.lazy()
        # ValidateEmail
        .with_columns(
            pl.when(to_invalid).then(pl.lit(False)).otherwise(pl.col(IS_OK)).alias(IS_OK),
            pl.when(to_invalid).then(pl.lit("INVALID_EMAIL")).otherwise(pl.col(ERROR_CODE)).alias(ERROR_CODE),
        )
        # ExtractEmailDomain
        .with_columns(
            pl.col(EMAIL).str.splitn("@", 2).struct.field("field_1").alias(EMAIL_DOMAIN)
        )
        # AssignCostEmail
        .with_columns(pl.lit(COST_VAL).alias(COST))
        # CustomMessage
        .with_columns(
            pl.format("Hola {}, tu código es 1234", pl.col(EMAIL).cast(pl.Utf8)).alias(MESSAGE)
        )
        # CustomSubject
        .with_columns(
            pl.format("Promoción para {}", pl.col(EMAIL).cast(pl.Utf8)).alias(SUBJECT)
        )
        # CalculateCreditsEmail
        .with_columns(
            pl.col(COST).cast(pl.Float64).round(3).alias(CREDITS)
        )
        .collect()
    )


# ------------------------------------------------------------------
# CalculatePDU — CURRENT (5 with_columns separados)
# ------------------------------------------------------------------

def pdu_current(df: pl.DataFrame) -> pl.DataFrame:
    msg = pl.col(MESSAGE)

    df = df.with_columns(
        msg.str.len_chars().alias(LENGTH),
        msg.str.len_bytes().alias(LENGTH_BYTES),
    )
    df = df.with_columns(
        (pl.col(LENGTH) != pl.col(LENGTH_BYTES)).alias(IS_SPECIAL)
    )
    df = df.with_columns(
        pl.when(pl.col(IS_SPECIAL)).then(pl.lit(PDU_SPC)).otherwise(pl.lit(PDU_STD)).alias(CREDIT_BASE),
        pl.when(pl.col(IS_SPECIAL)).then(pl.lit(OVH_SPC)).otherwise(pl.lit(OVH_STD)).alias(OVERHEAD),
    )
    df = df.with_columns(
        pl.when(pl.col(LENGTH) <= pl.col(CREDIT_BASE))
          .then(pl.col(CREDIT_BASE))
          .otherwise(pl.col(CREDIT_BASE) - pl.col(OVERHEAD))
          .alias(DIV)
    )
    return df.with_columns(
        (pl.col(LENGTH) / pl.col(DIV)).ceil().cast(pl.Int32).alias(PDU)
    )


# ------------------------------------------------------------------
# CalculatePDU — FUSED (expresiones inline, un solo with_columns)
# ------------------------------------------------------------------

def pdu_fused(df: pl.DataFrame) -> pl.DataFrame:
    msg       = pl.col(MESSAGE)
    msg_len   = msg.str.len_chars()
    msg_bytes = msg.str.len_bytes()
    is_spec   = msg_len != msg_bytes

    credit_base = pl.when(is_spec).then(pl.lit(PDU_SPC)).otherwise(pl.lit(PDU_STD))
    overhead    = pl.when(is_spec).then(pl.lit(OVH_SPC)).otherwise(pl.lit(OVH_STD))
    div_expr    = pl.when(msg_len <= credit_base).then(credit_base).otherwise(credit_base - overhead)

    return df.with_columns(
        msg_len.alias(LENGTH),
        msg_bytes.alias(LENGTH_BYTES),
        is_spec.alias(IS_SPECIAL),
        credit_base.alias(CREDIT_BASE),
        overhead.alias(OVERHEAD),
        div_expr.alias(DIV),
        (msg_len / div_expr).ceil().cast(pl.Int32).alias(PDU),
    )


# ------------------------------------------------------------------
# Runner
# ------------------------------------------------------------------

def bench(fn, df: pl.DataFrame, runs: int) -> float:
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn(df)
        times.append(time.perf_counter() - t0)
    return statistics.median(times)


def run():
    sep = "-" * 72

    # ── A: Email chain ────────────────────────────────────────────────────
    print("\n=== A) Email pure-transform chain (6 pasos) ===")
    print(f"{'Rows':>10}  {'Eager (s)':>10}  {'Lazy (s)':>10}  {'Δ':>8}  {'Ganancia':>10}")
    print(sep)

    for n in ROW_COUNTS:
        df = make_email_df(n)
        t_eager = bench(email_chain_eager, df, RUNS)
        t_lazy  = bench(email_chain_lazy,  df, RUNS)
        delta   = t_eager - t_lazy
        pct     = (delta / t_eager * 100) if t_eager > 0 else 0
        winner  = f"+{pct:.1f}%" if pct > 0 else f"{pct:.1f}%"
        print(f"{n:>10,}  {t_eager:>10.4f}  {t_lazy:>10.4f}  {delta:>+8.4f}  {winner:>10}")

    # ── B: CalculatePDU ───────────────────────────────────────────────────
    print("\n=== B) CalculatePDU (5 with_columns vs 1 fused) ===")
    print(f"{'Rows':>10}  {'5-pasos (s)':>12}  {'Fused (s)':>10}  {'Δ':>8}  {'Ganancia':>10}")
    print(sep)

    for n in ROW_COUNTS:
        df = make_sms_df(n)
        t_curr  = bench(pdu_current, df, RUNS)
        t_fused = bench(pdu_fused,   df, RUNS)
        delta   = t_curr - t_fused
        pct     = (delta / t_curr * 100) if t_curr > 0 else 0
        winner  = f"+{pct:.1f}%" if pct > 0 else f"{pct:.1f}%"
        print(f"{n:>10,}  {t_curr:>12.4f}  {t_fused:>10.4f}  {delta:>+8.4f}  {winner:>10}")

    print(f"\n(mediana de {RUNS} ejecuciones por celda)")


if __name__ == "__main__":
    run()
