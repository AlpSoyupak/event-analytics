import json
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.dependencies.rate_limit import require_tenant
from app.models.tenant import Tenant
from app.services.analytics_service import analytics_service


from app.services.cache_service import cache_service

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _days_ago(n: int) -> datetime:
    return _now() - timedelta(days=n)


@router.get("/cache-status")
async def get_cache_status(
    start: datetime = Query(default_factory=lambda: _days_ago(7)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
):
    """Returns the remaining Redis TTL (seconds) for the summary cache key.

    ttl_seconds == -2  → key does not exist (next request hits PostgreSQL)
    ttl_seconds == -1  → key exists but has no expiry (should not happen)
    ttl_seconds >= 0   → seconds until cached data is stale and PostgreSQL is queried
    """
    key = f"analytics:summary:{tenant.id}:{start.date()}:{end.date()}"
    ttl_seconds = await cache_service.ttl(key)
    from datetime import timezone
    expires_at = (
        (_now() + __import__("datetime").timedelta(seconds=ttl_seconds)).isoformat()
        if ttl_seconds >= 0 else None
    )
    return {"ttl_seconds": ttl_seconds, "expires_at": expires_at, "cache_key": key}


@router.get("/summary")
async def get_summary(
    start: datetime = Query(default_factory=lambda: _days_ago(7)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Aggregate event counts, unique users, and top event types for a time window."""
    return await analytics_service.get_summary(tenant.id, start, end, db)


@router.get("/timeseries")
async def get_timeseries(
    granularity: Literal["hour", "day", "week"] = Query("day"),
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    event_type: str | None = None,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Time-series breakdown. Uses materialized views for daily granularity."""
    return await analytics_service.get_timeseries(tenant.id, start, end, granularity, db, event_type)


@router.get("/top-events")
async def get_top_events(
    limit: int = Query(10, ge=1, le=100),
    start: datetime = Query(default_factory=lambda: _days_ago(7)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Top N most frequent event types with unique-user counts."""
    return await analytics_service.get_top_events(tenant.id, start, end, limit, db)


@router.get("/funnel")
async def get_funnel(
    steps: list[str] = Query(..., min_length=2, max_length=10),
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Sequential funnel analysis — tracks users through each step in order."""
    return await analytics_service.get_funnel(tenant.id, steps, start, end, db)


@router.get("/anomalies")
async def get_anomalies(tenant: Tenant = Depends(require_tenant)):
    """Return current anomaly alerts detected by the Celery worker (refreshed every 5 min)."""
    r = await cache_service.client()
    raw = await r.get(f"anomalies:{tenant.id}")
    anomalies = json.loads(raw) if raw else []
    return {"anomalies": anomalies, "checked_at": datetime.now(timezone.utc).isoformat()}


@router.get("/customers")
async def get_customer_segments(
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """User segments by purchase frequency (one_time / repeat / loyal) with LTV and revenue."""
    return await analytics_service.get_customer_segments(tenant.id, start, end, db)


@router.get("/search-gaps")
async def get_search_gaps(
    limit: int = Query(20, ge=1, le=100),
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Searches ranked by zero-result frequency — reveals catalog gaps."""
    return await analytics_service.get_search_gaps(tenant.id, start, end, limit, db)


@router.get("/basket")
async def get_basket_analysis(
    limit: int = Query(20, ge=1, le=100),
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Top co-purchased product pairs with attach rates for cross-sell recommendations."""
    return await analytics_service.get_basket_analysis(tenant.id, start, end, limit, db)


@router.get("/selling-times")
async def get_selling_times(
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Purchase volume and revenue by hour-of-day and day-of-week."""
    return await analytics_service.get_selling_times(tenant.id, start, end, db)


@router.get("/traffic-sources")
async def get_traffic_sources(
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Per-product conversion rates broken down by traffic source (referrer)."""
    return await analytics_service.get_traffic_sources(tenant.id, start, end, db)


@router.get("/experiments")
async def get_experiment_results(
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """A/B experiment results with per-variant conversion rates and uplift vs. control."""
    return await analytics_service.get_experiment_results(tenant.id, start, end, db)


@router.get("/revenue")
async def get_revenue(
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Total revenue, daily breakdown, and revenue by product category."""
    return await analytics_service.get_revenue(tenant.id, start, end, db)


@router.get("/products")
async def get_product_performance(
    limit: int = Query(20, ge=1, le=100),
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Per-product views, cart-adds, purchases, revenue, and conversion rates."""
    return await analytics_service.get_product_performance(tenant.id, start, end, limit, db)


@router.get("/retention")
async def get_retention(
    cohort_event: str,
    return_event: str,
    start: datetime = Query(default_factory=lambda: _days_ago(30)),
    end: datetime = Query(default_factory=_now),
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Day-N retention cohort analysis between two event types."""
    return await analytics_service.get_retention(tenant.id, start, end, cohort_event, return_event, db)
