import asyncio
import json
import logging
import uuid
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


@shared_task(bind=True, max_retries=1, default_retry_delay=60, queue="analytics")
def run_agent_evaluation(self, tenant_id: str):
    """Run the full LLM evaluation suite against a tenant's live data.

    Uses asyncio.run() to bridge Celery's sync context with the async agent
    and analytics service layers. Results are stored in Redis and retrievable
    via GET /api/v1/ai/eval/results.

    Schedule nightly via beat_schedule (add to celery_app.py):
        "nightly-agent-eval": {
            "task": "app.workers.tasks.run_agent_evaluation",
            "schedule": crontab(hour=3, minute=0),
            "args": [tenant_id],
        }
    """
    from app.database import async_session_factory
    from app.evaluation.runner import eval_runner

    async def _run() -> dict:
        async with async_session_factory() as db:
            return await eval_runner.run(uuid.UUID(tenant_id), db)

    try:
        report = asyncio.run(_run())
        logger.info(
            "agent_eval_complete",
            tenant_id=tenant_id,
            overall_score=report.get("overall_score"),
            cases_run=report.get("cases_run"),
        )
        return {"status": "ok", "overall_score": report["overall_score"]}
    except Exception as exc:
        logger.exception("run_agent_evaluation failed", tenant_id=tenant_id)
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=3, default_retry_delay=60, queue="analytics")
def detect_anomalies(self):
    """Z-score anomaly detection on hourly event counts across all tenants.

    Compares the most recent complete hour against a 23-hour rolling baseline.
    Spikes (z > 2.5) and drops (z < -2.5) are stored in Redis per tenant.
    """
    import math
    from collections import defaultdict

    import redis as sync_redis

    r = sync_redis.from_url(settings.redis_url)

    try:
        with _engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT tenant_id, event_type, hour, event_count
                    FROM mv_event_counts_hourly
                    WHERE hour >= NOW() - INTERVAL '24 hours'
                    ORDER BY tenant_id, event_type, hour
                """)
            )
            rows = [dict(row) for row in result.mappings()]

        grouped: dict = defaultdict(list)
        for row in rows:
            grouped[(str(row["tenant_id"]), row["event_type"])].append(
                (row["hour"], int(row["event_count"]))
            )

        anomalies_by_tenant: dict = defaultdict(list)

        for (tenant_id, event_type), points in grouped.items():
            points.sort(key=lambda x: x[0])
            if len(points) < 4:
                continue

            latest_hour, latest_count = points[-1]
            baseline = [c for _, c in points[:-1]]
            mean = sum(baseline) / len(baseline)

            if mean < 3:
                continue

            variance = sum((x - mean) ** 2 for x in baseline) / len(baseline)
            std = math.sqrt(variance)
            if std < 0.5:
                continue

            z = (latest_count - mean) / std
            if abs(z) < 2.5:
                continue

            kind = "spike" if z > 0 else "drop"
            anomalies_by_tenant[tenant_id].append({
                "event_type": event_type,
                "kind": kind,
                "hour": latest_hour.isoformat(),
                "count": latest_count,
                "expected": round(mean, 1),
                "z_score": round(z, 2),
                "description": (
                    f"{event_type} {kind}ed to {latest_count} events "
                    f"(expected ~{round(mean, 1)})"
                ),
            })

        for tenant_id, anomalies in anomalies_by_tenant.items():
            r.setex(f"anomalies:{tenant_id}", 7200, json.dumps(anomalies, default=str))

        total = sum(len(v) for v in anomalies_by_tenant.values())
        logger.info(f"Anomaly detection: {total} anomalies across {len(anomalies_by_tenant)} tenants")
        return {"anomalies_found": total}
    except Exception as exc:
        logger.exception("detect_anomalies failed")
        raise self.retry(exc=exc)


@shared_task(bind=True, max_retries=2, default_retry_delay=60, queue="analytics")
def replay_events(self, tenant_id: str, start: str, end: str, event_type: str | None = None):
    """Re-publish historical events to Kafka so the consumer pipeline re-processes them.

    Marks re-published events with source='replay' so the Kafka consumer inserts
    them as new rows (persisted=False). Capped at 10 000 events per job.
    """
    async def _run() -> int:
        import uuid as _uuid

        from sqlalchemy import select as _select

        from app.database import async_session_factory
        from app.models.event import Event
        from app.services.event_service import get_kafka_producer

        count = 0
        async with async_session_factory() as db:
            q = (
                _select(Event)
                .where(
                    Event.tenant_id == _uuid.UUID(tenant_id),
                    Event.received_at.between(start, end),
                    Event.source != "replay",
                )
                .order_by(Event.received_at)
                .limit(10_000)
            )
            if event_type:
                q = q.where(Event.event_type == event_type)
            result = await db.execute(q)
            events = result.scalars().all()

            producer = await get_kafka_producer()
            for event in events:
                await producer.send(
                    settings.kafka_events_topic,
                    value={
                        "event_id": str(event.id),
                        "tenant_id": str(event.tenant_id),
                        "event_type": event.event_type,
                        "user_id": event.user_id,
                        "session_id": event.session_id,
                        "properties": event.properties,
                        "received_at": event.received_at.isoformat(),
                        "source": "replay",
                        "persisted": False,
                        "meta": {"replayed": True, "original_event_id": str(event.id)},
                    },
                    key=str(event.tenant_id).encode(),
                )
                count += 1
        return count

    try:
        count = asyncio.run(_run())
        logger.info(f"Replayed {count} events for tenant {tenant_id}")
        return {"replayed": count, "tenant_id": tenant_id}
    except Exception as exc:
        logger.exception("replay_events failed")
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
