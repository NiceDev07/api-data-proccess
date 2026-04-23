"""
Polars performance benchmark — Email pipeline transforms + CalculatePDU + _build_summary + archivo real.

Compara:
  A) Email pure-transform chain: 6 pasos eager (step-by-step) vs lazy vs lazy+streaming
  B) CalculatePDU: 5 with_columns separados vs 1 expresión fused
  C) _build_summary: 2 passes materializados vs collect_all (paralelo)
  D) Archivo real 1M: read_csv vs scan_csv+collect vs scan_csv+streaming
  E) Pipeline completo sobre archivo real: pre-steps + transform (eager / lazy / streaming)

Conclusiones clave:
  - Lazy > Eager (~30-47% en 1M) → usar lazy para transform chain ✓
  - Fused es MÁS LENTO para PDU → Polars reutiliza columnas intermedias ✓
  - engine="streaming" reduce pico de memoria (importante en alta concurrencia)
  - collect_all paraleliza general + group stats (ahorra ~50% wall-clock en _build_summary)

Usage:
    uv run python benchmark_polars.py
"""
import time
import statistics

import polars as pl

# ------------------------------------------------------------------
# Column names (same as Cols constants)
# ------------------------------------------------------------------
EMAIL        = "__EMAIL__"
IS_OK        = "__IS_OK__"
ERROR_CODE   = "__ERROR_CODE__"
EMAIL_DOMAIN = "__EMAIL_DOMAIN__"
COST         = "__COST__"
MESSAGE      = "__message__"
SUBJECT      = "__SUBJECT__"
CREDITS      = "__CREDITS__"
LENGTH       = "__LEN__"
LENGTH_BYTES = "__LENB__"
IS_SPECIAL   = "__IS_SPECIAL__"
CREDIT_BASE  = "__CREDIT_BASE__"
OVERHEAD     = "__OVER_HEAD__"
DIV          = "__DIV__"
PDU          = "__PDU__"

EMAIL_RE  = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
COST_VAL  = 0.025
CONTENT   = "Hola {__EMAIL__}, tu código es 1234"
SUBJ_TMPL = "Promoción para {__EMAIL__}"

PDU_STD, OVH_STD = 160, 7
PDU_SPC, OVH_SPC = 70,  3

RUNS       = 5
ROW_COUNTS = [10_000, 100_000, 500_000, 1_000_000]

KNOWN_DOMAINS = frozenset(["gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "icloud.com"])
OTHER = "others"

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
    return pl.DataFrame({MESSAGE: [msgs[i % len(msgs)] for i in range(n)]})


def make_processed_email_df(n: int) -> pl.DataFrame:
    """DataFrame post-pipeline (tiene domain + cost + credits + is_ok)."""
    base = ["user@gmail.com", "otro@hotmail.com", "test@yahoo.com", "real@outlook.com", "x@icloud.com"]
    doms = ["gmail.com",      "hotmail.com",      "yahoo.com",      "outlook.com",      "icloud.com"]
    valid_flags = [True, True, True, False, True]  # ~80 % válidos
    emails  = [base[i % len(base)] for i in range(n)]
    domains = [doms[i % len(doms)] for i in range(n)]
    is_ok   = [valid_flags[i % len(valid_flags)] for i in range(n)]
    return pl.DataFrame({
        EMAIL:        emails,
        IS_OK:        is_ok,
        ERROR_CODE:   [None] * n,
        EMAIL_DOMAIN: domains,
        COST:         [COST_VAL] * n,
        CREDITS:      [COST_VAL] * n,
    }).with_columns(pl.col(ERROR_CODE).cast(pl.Utf8))


# ------------------------------------------------------------------
# A: Email chain — EAGER (step by step)
# ------------------------------------------------------------------

def email_chain_eager(df: pl.DataFrame) -> pl.DataFrame:
    is_valid   = pl.col(EMAIL).str.contains(EMAIL_RE)
    to_invalid = pl.col(IS_OK) & ~is_valid
    df = df.with_columns(
        pl.when(to_invalid).then(pl.lit(False)).otherwise(pl.col(IS_OK)).alias(IS_OK),
        pl.when(to_invalid).then(pl.lit("INVALID_EMAIL")).otherwise(pl.col(ERROR_CODE)).alias(ERROR_CODE),
    )
    df = df.with_columns(pl.col(EMAIL).str.splitn("@", 2).struct.field("field_1").alias(EMAIL_DOMAIN))
    df = df.with_columns(pl.lit(COST_VAL).alias(COST))
    df = df.with_columns(pl.format("Hola {}, tu código es 1234", pl.col(EMAIL).cast(pl.Utf8)).alias(MESSAGE))
    df = df.with_columns(pl.format("Promoción para {}", pl.col(EMAIL).cast(pl.Utf8)).alias(SUBJECT))
    df = df.with_columns(pl.col(COST).cast(pl.Float64).round(3).alias(CREDITS))
    return df


# ------------------------------------------------------------------
# A: Email chain — LAZY (single plan, collect eager)
# ------------------------------------------------------------------

def email_chain_lazy(df: pl.DataFrame) -> pl.DataFrame:
    is_valid   = pl.col(EMAIL).str.contains(EMAIL_RE)
    to_invalid = pl.col(IS_OK) & ~is_valid
    return (
        df.lazy()
        .with_columns(
            pl.when(to_invalid).then(pl.lit(False)).otherwise(pl.col(IS_OK)).alias(IS_OK),
            pl.when(to_invalid).then(pl.lit("INVALID_EMAIL")).otherwise(pl.col(ERROR_CODE)).alias(ERROR_CODE),
        )
        .with_columns(pl.col(EMAIL).str.splitn("@", 2).struct.field("field_1").alias(EMAIL_DOMAIN))
        .with_columns(pl.lit(COST_VAL).alias(COST))
        .with_columns(pl.format("Hola {}, tu código es 1234", pl.col(EMAIL).cast(pl.Utf8)).alias(MESSAGE))
        .with_columns(pl.format("Promoción para {}", pl.col(EMAIL).cast(pl.Utf8)).alias(SUBJECT))
        .with_columns(pl.col(COST).cast(pl.Float64).round(3).alias(CREDITS))
        .collect()
    )


# ------------------------------------------------------------------
# A: Email chain — LAZY + STREAMING (engine="streaming")
# Reduce pico de memoria en alta concurrencia, ligero overhead de CPU.
# ------------------------------------------------------------------

def email_chain_streaming(df: pl.DataFrame) -> pl.DataFrame:
    is_valid   = pl.col(EMAIL).str.contains(EMAIL_RE)
    to_invalid = pl.col(IS_OK) & ~is_valid
    return (
        df.lazy()
        .with_columns(
            pl.when(to_invalid).then(pl.lit(False)).otherwise(pl.col(IS_OK)).alias(IS_OK),
            pl.when(to_invalid).then(pl.lit("INVALID_EMAIL")).otherwise(pl.col(ERROR_CODE)).alias(ERROR_CODE),
        )
        .with_columns(pl.col(EMAIL).str.splitn("@", 2).struct.field("field_1").alias(EMAIL_DOMAIN))
        .with_columns(pl.lit(COST_VAL).alias(COST))
        .with_columns(pl.format("Hola {}, tu código es 1234", pl.col(EMAIL).cast(pl.Utf8)).alias(MESSAGE))
        .with_columns(pl.format("Promoción para {}", pl.col(EMAIL).cast(pl.Utf8)).alias(SUBJECT))
        .with_columns(pl.col(COST).cast(pl.Float64).round(3).alias(CREDITS))
        .collect(engine="streaming")
    )


# ------------------------------------------------------------------
# B: CalculatePDU — CURRENT (5 with_columns separados)
# ------------------------------------------------------------------

def pdu_current(df: pl.DataFrame) -> pl.DataFrame:
    msg = pl.col(MESSAGE)
    df = df.with_columns(msg.str.len_chars().alias(LENGTH), msg.str.len_bytes().alias(LENGTH_BYTES))
    df = df.with_columns((pl.col(LENGTH) != pl.col(LENGTH_BYTES)).alias(IS_SPECIAL))
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
    return df.with_columns((pl.col(LENGTH) / pl.col(DIV)).ceil().cast(pl.Int32).alias(PDU))


# ------------------------------------------------------------------
# B: CalculatePDU — FUSED (expresiones inline, un solo with_columns)
# NOTA: más lento que separado — Polars no cachea sub-exprs repetidas dentro
# de un mismo with_columns; las columnas intermedias sirven de caché natural.
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
# C: _build_summary — ACTUAL (2 passes sobre valid: group_by + select)
# ------------------------------------------------------------------

def build_summary_current(df: pl.DataFrame):
    valid = df.filter(pl.col(IS_OK))
    valid = valid.with_columns(
        pl.when(pl.col(EMAIL_DOMAIN).is_in(list(KNOWN_DOMAINS)))
        .then(pl.col(EMAIL_DOMAIN))
        .otherwise(pl.lit(OTHER))
        .alias(EMAIL_DOMAIN)
    )
    group_df = (
        valid.group_by(EMAIL_DOMAIN)
        .agg(
            pl.len().alias("total"),
            pl.col(COST).first().alias("unit_value"),
            pl.col(CREDITS).sum().alias("credits"),
        )
        .sort("credits", descending=True)
    )
    general_row = valid.select(
        pl.len().alias("total_records"),
        pl.col(CREDITS).sum().alias("total_credits"),
    ).row(0, named=True)
    return group_df, general_row


# ------------------------------------------------------------------
# C: _build_summary — collect_all (group + general en paralelo)
# ------------------------------------------------------------------

def build_summary_collect_all(df: pl.DataFrame):
    valid_lf = df.lazy().filter(pl.col(IS_OK))
    normalized = (
        pl.when(pl.col(EMAIL_DOMAIN).is_in(list(KNOWN_DOMAINS)))
        .then(pl.col(EMAIL_DOMAIN))
        .otherwise(pl.lit(OTHER))
    )
    general_lf = valid_lf.select(
        pl.len().alias("total_records"),
        pl.col(CREDITS).sum().alias("total_credits"),
    )
    group_lf = (
        valid_lf
        .with_columns(normalized.alias(EMAIL_DOMAIN))
        .group_by(EMAIL_DOMAIN)
        .agg(
            pl.len().alias("total"),
            pl.col(COST).first().alias("unit_value"),
            pl.col(CREDITS).sum().alias("credits"),
        )
        .sort("credits", descending=True)
    )
    return pl.collect_all([general_lf, group_lf])


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
    sep = "-" * 82

    # ── A: Email chain ────────────────────────────────────────────────────────────
    print("\n=== A) Email transform chain (6 pasos) ===")
    print(f"{'Rows':>10}  {'Eager (s)':>10}  {'Lazy (s)':>10}  {'Stream (s)':>11}  {'Lazy Δ%':>8}  {'Stream Δ%':>10}")
    print(sep)
    for n in ROW_COUNTS:
        df = make_email_df(n)
        t_eager  = bench(email_chain_eager,     df, RUNS)
        t_lazy   = bench(email_chain_lazy,      df, RUNS)
        t_stream = bench(email_chain_streaming, df, RUNS)
        pct_lazy   = (t_eager - t_lazy)   / t_eager * 100 if t_eager > 0 else 0
        pct_stream = (t_eager - t_stream) / t_eager * 100 if t_eager > 0 else 0
        print(
            f"{n:>10,}  {t_eager:>10.4f}  {t_lazy:>10.4f}  {t_stream:>11.4f}"
            f"  {pct_lazy:>+7.1f}%  {pct_stream:>+9.1f}%"
        )

    print("\n  Nota: 'Stream Δ%' negativo = más lento que eager pero usa MENOS memoria pico.")
    print("  Alta concurrencia → preferir streaming para evitar OOM con múltiples requests 1M.")

    # ── B: CalculatePDU ───────────────────────────────────────────────────────────
    print("\n=== B) CalculatePDU (5 with_columns separados vs 1 fused) ===")
    print(f"{'Rows':>10}  {'5-pasos (s)':>12}  {'Fused (s)':>10}  {'Δ':>8}  {'Ganancia':>10}")
    print(sep)
    for n in ROW_COUNTS:
        df     = make_sms_df(n)
        t_curr  = bench(pdu_current, df, RUNS)
        t_fused = bench(pdu_fused,   df, RUNS)
        delta   = t_curr - t_fused
        pct     = (delta / t_curr * 100) if t_curr > 0 else 0
        winner  = f"+{pct:.1f}%" if pct > 0 else f"{pct:.1f}%"
        print(f"{n:>10,}  {t_curr:>12.4f}  {t_fused:>10.4f}  {delta:>+8.4f}  {winner:>10}")
    print("\n  Nota: fused es más lento porque Polars no cachea sub-exprs dentro del mismo")
    print("  with_columns. Las columnas intermedias actúan como caché natural → mantener separados.")

    # ── C: _build_summary ─────────────────────────────────────────────────────────
    print("\n=== C) _build_summary: 2-passes vs collect_all paralelo ===")
    print(f"{'Rows':>10}  {'2-passes (s)':>13}  {'collect_all (s)':>15}  {'Δ':>8}  {'Ganancia':>10}")
    print(sep)
    for n in ROW_COUNTS:
        df      = make_processed_email_df(n)
        t_curr  = bench(build_summary_current,     df, RUNS)
        t_coll  = bench(build_summary_collect_all, df, RUNS)
        delta   = t_curr - t_coll
        pct     = (delta / t_curr * 100) if t_curr > 0 else 0
        winner  = f"+{pct:.1f}%" if pct > 0 else f"{pct:.1f}%"
        print(f"{n:>10,}  {t_curr:>13.4f}  {t_coll:>15.4f}  {delta:>+8.4f}  {winner:>10}")

    # ── D: File loading strategies ────────────────────────────────────────────────
    csv_path = "files/1M_emails_dummy.csv"
    import os
    if os.path.exists(csv_path):
        print(f"\n=== D) Carga de archivo real ({csv_path}) ===")
        print(f"  {'Estrategia':<35}  {'Mediana (s)':>11}  {'Filas':>9}  {'MB pico aprox':>14}")
        print(sep)

        def load_read_csv():
            return pl.read_csv(csv_path)

        def load_scan_collect():
            return pl.scan_csv(csv_path).collect()

        def load_scan_streaming():
            return pl.scan_csv(csv_path).collect(engine="streaming")

        def bench0(fn, runs: int) -> float:
            times = []
            for _ in range(runs):
                t0 = time.perf_counter()
                fn()
                times.append(time.perf_counter() - t0)
            return statistics.median(times)

        loaders = [
            ("read_csv (eager)",          load_read_csv),
            ("scan_csv + collect()",      load_scan_collect),
            ("scan_csv + streaming",      load_scan_streaming),
        ]
        for name, fn in loaders:
            t = bench0(fn, RUNS)
            df_sample = fn()
            rows = len(df_sample)
            mb = df_sample.estimated_size("mb")
            print(f"  {name:<35}  {t:>11.4f}  {rows:>9,}  {mb:>13.1f}MB")

        # ── E: Full email pipeline on real file ───────────────────────────────────
        print(f"\n=== E) Pipeline completo sobre archivo real ({csv_path}) ===")
        print("  (CleanData equivalent + ExclutionEmail skip + 6 transform steps)")
        print(f"  {'Estrategia':<42}  {'Mediana (s)':>11}")
        print(sep)

        def _pre_process(raw_df: pl.DataFrame) -> pl.DataFrame:
            """Simula CleanDataEmail + ExclutionEmail (sin lista → todos válidos)."""
            col = "email"
            return (
                raw_df
                .with_columns(
                    pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase()
                    .replace("", None).alias(col)
                )
                .filter(pl.col(col).is_not_null())
                .rename({col: EMAIL})
                .with_columns(
                    pl.lit(True).alias(IS_OK),
                    pl.lit(None).cast(pl.Utf8).alias(ERROR_CODE),
                )
            )

        def _transform_eager(df: pl.DataFrame) -> pl.DataFrame:
            return email_chain_eager(df)

        def _transform_lazy(df: pl.DataFrame) -> pl.DataFrame:
            return email_chain_lazy(df)

        def _transform_streaming(df: pl.DataFrame) -> pl.DataFrame:
            return email_chain_streaming(df)

        raw_df = pl.read_csv(csv_path)
        base_df = _pre_process(raw_df)

        strategies = [
            ("pre (read_csv) + transform eager",     lambda: _transform_eager(base_df)),
            ("pre (read_csv) + transform lazy",      lambda: _transform_lazy(base_df)),
            ("pre (read_csv) + transform streaming", lambda: _transform_streaming(base_df)),
        ]

        # Full end-to-end including file read
        def full_eager():
            df = _pre_process(pl.read_csv(csv_path))
            return _transform_eager(df)

        def full_lazy():
            df = _pre_process(pl.read_csv(csv_path))
            return _transform_lazy(df)

        def full_scan_streaming():
            df = _pre_process(pl.scan_csv(csv_path).collect(engine="streaming"))
            return _transform_streaming(df)

        strategies += [
            ("--- end-to-end (incl. I/O) ---",       None),
            ("read_csv + eager transform",            full_eager),
            ("read_csv + lazy transform",             full_lazy),
            ("scan_csv+stream + stream transform",    full_scan_streaming),
        ]

        for name, fn in strategies:
            if fn is None:
                print(f"\n  {name}")
                continue
            t = bench0(fn, RUNS)
            print(f"  {name:<42}  {t:>11.4f}s")
    else:
        print(f"\n  [skip D/E] archivo no encontrado: {csv_path}")

    print(f"\n(mediana de {RUNS} ejecuciones por celda)")


if __name__ == "__main__":
    run()
