"""
Gunicorn production configuration — api-data-process.
https://docs.gunicorn.org/en/stable/settings.html
"""
import multiprocessing
import os

# ── Bind ──────────────────────────────────────────────────────────────────────
# Escucha en todas las interfaces para ser accesible desde la LAN.
# Ajustar HOST en .env si se quiere restringir a una interfaz específica.
_host = os.getenv("HOST", "0.0.0.0")
_port = os.getenv("PORT", "3000")
bind  = f"{_host}:{_port}"

# ── Workers ───────────────────────────────────────────────────────────────────
# UvicornWorker runs a full asyncio event loop per worker process.
# For async I/O-heavy apps, cpu_count workers is appropriate.
# Increase GUNICORN_WORKERS if you have CPU headroom and higher concurrency.
workers      = int(os.getenv("GUNICORN_WORKERS", multiprocessing.cpu_count()))
worker_class = "uvicorn.workers.UvicornWorker"

# ── Timeouts ──────────────────────────────────────────────────────────────────
# Processing up to 700 k-row CSV/XLSX files can take several minutes.
# GUNICORN_TIMEOUT must be >= the worst-case pipeline duration.
timeout         = int(os.getenv("GUNICORN_TIMEOUT", "300"))  # 5 min hard kill
graceful_timeout = 30    # seconds to drain in-flight requests on SIGTERM
keepalive        = 5     # HTTP keep-alive seconds

# ── Connections ───────────────────────────────────────────────────────────────
worker_connections = 1000

# ── Process naming ────────────────────────────────────────────────────────────
proc_name = "api-data-process"

# ── App entrypoint ────────────────────────────────────────────────────────────
# main.py lives in src/; chdir sets the working directory so Python finds it.
_src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
chdir    = _src
wsgi_app = "main:app"

# ── Preload ───────────────────────────────────────────────────────────────────
# MUST remain False.
# Async SQLAlchemy engines and the Redis pool are created inside lifespan().
# If preload_app were True, those objects would be created before fork() and
# become corrupted in the child workers (shared file-descriptors / event loops).
preload_app = False

# ── Logging ───────────────────────────────────────────────────────────────────
# Write to stdout/stderr so journald captures everything without extra config.
loglevel          = os.getenv("LOG_LEVEL", "info")
accesslog         = "-"
errorlog          = "-"
access_log_format = '%(h)s %(l)s %(u)s "%(r)s" %(s)s %(b)sB %(D)sµs'

# ── Request limits ────────────────────────────────────────────────────────────
limit_request_line        = 8190
limit_request_fields      = 200
limit_request_field_size  = 8190
