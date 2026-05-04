from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.database import engine
from app.middleware.logging import RequestLoggingMiddleware
from app.routers import ai, analytics, events, tenants
from app.services.cache_service import cache_service
from app.services.event_service import close_kafka_producer, get_kafka_producer

settings = get_settings()
logger = structlog.get_logger()

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.JSONRenderer(),
    ],
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("startup", environment=settings.environment)
    try:
        await get_kafka_producer()
    except Exception:
        logger.warning("kafka_unavailable", detail="Events will not be published to Kafka")
    yield
    await close_kafka_producer()
    await cache_service.close()
    await engine.dispose()
    logger.info("shutdown_complete")


app = FastAPI(
    title="Event Analytics Platform",
    description=(
        "Real-time event ingestion and analytics with multi-tenant isolation.\n\n"
        "**Auth:** pass your tenant API key in the `X-API-Key` header.\n\n"
        "**Rate limits:** enforced per tenant via Redis sliding window (headers: `X-RateLimit-*`)."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-RateLimit-Limit", "X-RateLimit-Remaining", "X-Response-Time"],
)
app.add_middleware(RequestLoggingMiddleware)

app.include_router(events.router, prefix="/api/v1")
app.include_router(analytics.router, prefix="/api/v1")
app.include_router(tenants.router, prefix="/api/v1")
app.include_router(ai.router, prefix="/api/v1")

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/dashboard", include_in_schema=False)
async def dashboard():
    return FileResponse("app/static/dashboard.html")


@app.get("/health", tags=["ops"])
async def health():
    return {"status": "healthy", "version": "1.0.0", "environment": settings.environment}


@app.get("/ready", tags=["ops"])
async def readiness(request: Request):
    """Checks DB + Redis connectivity — used by orchestrators."""
    from sqlalchemy import text

    from app.database import async_session_factory

    checks: dict[str, str] = {}
    ok = True

    try:
        async with async_session_factory() as db:
            await db.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = str(e)
        ok = False

    try:
        r = await cache_service.client()
        await r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = str(e)
        ok = False

    status_code = 200 if ok else 503
    return JSONResponse({"status": "ready" if ok else "degraded", "checks": checks}, status_code=status_code)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("unhandled_error", path=str(request.url))
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
