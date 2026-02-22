from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from backend.api.endpoints import router as api_router
from backend.core.config import settings
from backend.core.logging_config import setup_logging, generate_request_id, request_id_var
import logging
import os

setup_logging()
logger = logging.getLogger(__name__)


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Inject a unique request_id into every request for log correlation."""

    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or generate_request_id()
        token = request_id_var.set(rid)
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        request_id_var.reset(token)
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle — validate configuration on boot."""
    api_warnings = settings.validate_api_keys()
    for warning in api_warnings:
        logger.warning("CONFIG: %s", warning)

    from backend.engine.tariff_engine import tariff_engine
    if tariff_engine.ruleset is None:
        logger.error("STARTUP: Tariff rules failed to load — calculation endpoints will return empty results")
    else:
        logger.info(
            "STARTUP: Tariff rules loaded — %d sections, version=%s",
            len(tariff_engine.ruleset.sections), tariff_engine.version,
        )
    yield


app = FastAPI(
    title="Marc · Port Tariff API",
    description="Marc — Deterministic computation of port dues with auditable citations and LLM-assisted ingestion",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Custom 422 error handler with clear validation messages ──────────────────

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Return clear, structured validation errors for 422 responses."""
    errors = []
    for err in exc.errors():
        loc = " → ".join(str(part) for part in err.get("loc", []))
        errors.append({
            "field": loc,
            "message": err.get("msg", ""),
            "type": err.get("type", "value_error"),
        })
    return JSONResponse(
        status_code=422,
        content={"detail": errors},
    )


# Configure CORS — configurable via CORS_ORIGINS env var (comma-separated).
# Defaults to ["*"] so any frontend host/port works out of the box.
# Example: CORS_ORIGINS=http://localhost:5173,https://app.example.com
_raw_origins = os.environ.get("CORS_ORIGINS", "*")
_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(RequestIdMiddleware)

app.include_router(api_router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {"message": "Marc Port Tariff API", "docs": "/docs"}

@app.get("/health")
def health_check():
    from backend.engine.tariff_engine import tariff_engine

    if tariff_engine.ruleset is None:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "detail": "Tariff rules not loaded"},
        )
    return {
        "status": "healthy",
        "tariff_version": tariff_engine.version,
        "sections": len(tariff_engine.ruleset.sections),
    }


@app.get("/ready")
def readiness_check():
    """Dependency readiness check — verifies YAML loaded and audit store writable."""
    from backend.engine.tariff_engine import tariff_engine
    from pathlib import Path

    checks: dict[str, str] = {}

    if tariff_engine.ruleset is not None:
        checks["yaml"] = "ok"
    else:
        checks["yaml"] = "not_loaded"

    audit_dir = Path(settings.AUDIT_LOG_DIR)
    if audit_dir.is_dir() and os.access(audit_dir, os.W_OK):
        checks["audit"] = "ok"
    else:
        checks["audit"] = "not_writable"

    if settings.GEMINI_API_KEY:
        checks["gemini_key"] = "configured"
    else:
        checks["gemini_key"] = "missing"

    is_ready = checks["yaml"] == "ok" and checks["audit"] == "ok"
    status_code = 200 if is_ready else 503

    return JSONResponse(
        status_code=status_code,
        content={"status": "ready" if is_ready else "not_ready", "checks": checks},
    )
