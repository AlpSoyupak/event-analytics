import json
import logging
from datetime import datetime, timedelta, timezone

from celery import shared_task
from sqlalchemy import create_engine, text

from app.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# Synchronous SQLAlchemy engine for Celery (Celery workers are not async)
_engine = create_engine(
    settings.sync_database_url,
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
)


@shared_task(bind=True, max_retries=3, default_retry_delay=60, queue="maintenance")
def refresh_materialized_views(self):
    """Concurrently refresh hourly + daily materialized views.

    CONCURRENTLY means reads are not blocked during refresh — safe for production.
    Requires the unique indexes created in the initial migration.
    """
    try:
        with _engine.connect() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_event_counts_hourly"))
            conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_event_counts_daily"))
            conn.commit()
        logger.info("Materialized views refreshed")
        return {"status": "ok"}
    except Exception as exc:
        logger.exception("Failed to refresh materialized views")
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=3, default_retry_delay=30, queue="analytics")
def precompute_tenant_analytics(self, tenant_id: str, start: str, end: str):
    """Pre-warm Redis cache for a tenant's analytics dashboard.

    Triggered on-demand (e.g. after a large batch ingest) so the first
    dashboard load is instant.
    """
    import redis as sync_redis

    r = sync_redis.from_url(settings.redis_url)

    try:
        with _engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT
                        date_trunc('day', received_at) AS day,
                        COUNT(*)                        AS events,
                        COUNT(DISTINCT user_id)         AS unique_users,
                        jsonb_object_agg(event_type, cnt) AS breakdown
                    FROM (
                        SELECT received_at, user_id, event_type,
                               COUNT(*) OVER (PARTITION BY date_trunc('day', received_at), event_type) AS cnt
                        FROM events
                        WHERE tenant_id = :tid
                          AND received_at BETWEEN :start AND :end
                    ) sub
                    GROUP BY date_trunc('day', received_at)
                    ORDER BY day
                """),
                {"tid": tenant_id, "start": start, "end": end},
            )
            rows = [dict(row) for row in result.mappings()]

        cache_key = f"precomputed:{tenant_id}:{start[:10]}:{end[:10]}"
        r.setex(cache_key, 3600, json.dumps(rows, default=str))
        logger.info(f"Precomputed {len(rows)} days for tenant {tenant_id}")
        return {"tenant_id": tenant_id, "days": len(rows)}
    except Exception as exc:
        logger.exception("precompute_tenant_analytics failed")
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=3, default_retry_delay=120, queue="maintenance")
def cleanup_old_events(self):
    """Hard-delete events past the configured retention window.

    Runs nightly. Uses a DELETE with LIMIT to avoid long-running locks.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.event_retention_days)
    total_deleted = 0

    try:
        with _engine.connect() as conn:
            # Batch deletes to avoid table-level pressure
            while True:
                result = conn.execute(
                    text("""
                        DELETE FROM events
                        WHERE id IN (
                            SELECT id FROM events
                            WHERE received_at < :cutoff
                            LIMIT 10000
                        )
                    """),
                    {"cutoff": cutoff},
                )
                conn.commit()
                batch = result.rowcount
                total_deleted += batch
                if batch < 10000:
                    break

        logger.info(f"Deleted {total_deleted} events older than {settings.event_retention_days} days")
        return {"deleted": total_deleted}
    except Exception as exc:
        logger.exception("cleanup_old_events failed")
        raise self.retry(exc=exc)
