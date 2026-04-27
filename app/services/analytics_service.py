import uuid
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.services.cache_service import cache_service

settings = get_settings()

_TRUNC = {"hour": "hour", "day": "day", "week": "week"}


class AnalyticsService:
    async def get_summary(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        db: AsyncSession,
    ) -> dict:
        key = f"analytics:summary:{tenant_id}:{start.date()}:{end.date()}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                SELECT
                    SUM(type_count)              AS total_events,
                    SUM(type_users)              AS unique_users,
                    SUM(type_sessions)           AS unique_sessions,
                    json_agg(
                        json_build_object(
                            'event_type',   event_type,
                            'count',        type_count,
                            'unique_users', type_users
                        ) ORDER BY type_count DESC
                    ) AS event_types
                FROM (
                    SELECT
                        event_type,
                        COUNT(*)                AS type_count,
                        COUNT(DISTINCT user_id) AS type_users,
                        COUNT(DISTINCT session_id) AS type_sessions
                    FROM events
                    WHERE tenant_id = :tid
                      AND received_at BETWEEN :start AND :end
                    GROUP BY event_type
                ) t
            """),
            {"tid": str(tenant_id), "start": start, "end": end},
        )
        row = result.mappings().first()
        total = int(row["total_events"] or 0)
        event_types = row["event_types"] or []

        data = {
            "tenant_id": str(tenant_id),
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "total_events": total,
            "unique_users": int(row["unique_users"] or 0),
            "unique_sessions": int(row["unique_sessions"] or 0),
            "event_types": [
                {**et, "percentage": round(et["count"] / total * 100, 2) if total else 0.0}
                for et in event_types
            ],
        }
        await cache_service.set(key, data, ttl=settings.summary_cache_ttl)
        return data

    async def get_timeseries(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        granularity: str,
        db: AsyncSession,
        event_type: str | None = None,
    ) -> list[dict]:
        key = f"analytics:ts:{tenant_id}:{start.date()}:{end.date()}:{granularity}:{event_type or '*'}"
        if cached := await cache_service.get(key):
            return cached

        trunc = _TRUNC.get(granularity, "day")

        # Prefer the materialized view for daily granularity (faster, pre-aggregated)
        if granularity == "day" and event_type is None:
            query = text("""
                SELECT
                    day                  AS ts,
                    SUM(event_count)     AS event_count,
                    SUM(unique_users)    AS unique_users,
                    SUM(unique_sessions) AS unique_sessions
                FROM mv_event_counts_daily
                WHERE tenant_id = :tid
                  AND day BETWEEN :start AND :end
                GROUP BY day
                ORDER BY day
            """)
            params: dict = {"tid": str(tenant_id), "start": start, "end": end}
        else:
            et_clause = "AND event_type = :event_type" if event_type else ""
            query = text(f"""
                SELECT
                    date_trunc('{trunc}', received_at) AS ts,
                    COUNT(*)                           AS event_count,
                    COUNT(DISTINCT user_id)            AS unique_users,
                    COUNT(DISTINCT session_id)         AS unique_sessions
                FROM events
                WHERE tenant_id = :tid
                  AND received_at BETWEEN :start AND :end
                  {et_clause}
                GROUP BY date_trunc('{trunc}', received_at)
                ORDER BY ts
            """)
            params = {"tid": str(tenant_id), "start": start, "end": end}
            if event_type:
                params["event_type"] = event_type

        result = await db.execute(query, params)
        data = [
            {
                "timestamp": row["ts"].isoformat(),
                "event_count": int(row["event_count"]),
                "unique_users": int(row["unique_users"]),
                "unique_sessions": int(row["unique_sessions"]),
            }
            for row in result.mappings()
        ]
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_top_events(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        limit: int,
        db: AsyncSession,
    ) -> list[dict]:
        key = f"analytics:top:{tenant_id}:{start.date()}:{end.date()}:{limit}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                SELECT
                    event_type,
                    COUNT(*)                AS count,
                    COUNT(DISTINCT user_id) AS unique_users
                FROM events
                WHERE tenant_id = :tid
                  AND received_at BETWEEN :start AND :end
                GROUP BY event_type
                ORDER BY count DESC
                LIMIT :lim
            """),
            {"tid": str(tenant_id), "start": start, "end": end, "lim": limit},
        )
        data = [dict(row) for row in result.mappings()]
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_funnel(
        self,
        tenant_id: uuid.UUID,
        steps: list[str],
        start: datetime,
        end: datetime,
        db: AsyncSession,
    ) -> dict:
        key = f"analytics:funnel:{tenant_id}:{':'.join(steps)}:{start.date()}:{end.date()}"
        if cached := await cache_service.get(key):
            return cached

        # Build CTEs tracking users through each funnel step in sequence
        cte_parts = []
        for i, step in enumerate(steps):
            if i == 0:
                cte_parts.append(f"""
                    step_{i} AS (
                        SELECT user_id, MIN(received_at) AS ts
                        FROM events
                        WHERE tenant_id = :tid
                          AND event_type = :s{i}
                          AND received_at BETWEEN :start AND :end
                          AND user_id IS NOT NULL
                        GROUP BY user_id
                    )""")
            else:
                cte_parts.append(f"""
                    step_{i} AS (
                        SELECT e.user_id, MIN(e.received_at) AS ts
                        FROM events e
                        JOIN step_{i - 1} prev ON e.user_id = prev.user_id
                        WHERE e.tenant_id = :tid
                          AND e.event_type = :s{i}
                          AND e.received_at > prev.ts
                          AND e.received_at BETWEEN :start AND :end
                        GROUP BY e.user_id
                    )""")

        select_counts = ", ".join(
            f"(SELECT COUNT(*) FROM step_{i}) AS c{i}" for i in range(len(steps))
        )
        sql = "WITH " + ",\n".join(cte_parts) + f"\nSELECT {select_counts}"

        params: dict = {"tid": str(tenant_id), "start": start, "end": end}
        for i, step in enumerate(steps):
            params[f"s{i}"] = step

        result = await db.execute(text(sql), params)
        row = result.mappings().first()
        counts = [int(row[f"c{i}"]) for i in range(len(steps))]

        entered = counts[0] if counts else 0
        funnel_steps = [
            {
                "event_type": step,
                "count": count,
                "conversion_rate": round(count / counts[i - 1] * 100, 2) if i > 0 and counts[i - 1] else 100.0,
            }
            for i, (step, count) in enumerate(zip(steps, counts))
        ]

        data = {
            "tenant_id": str(tenant_id),
            "steps": funnel_steps,
            "total_entered": entered,
            "total_completed": counts[-1] if counts else 0,
            "overall_conversion": round(counts[-1] / entered * 100, 2) if entered else 0.0,
        }
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_retention(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        cohort_event: str,
        return_event: str,
        db: AsyncSession,
    ) -> list[dict]:
        """Day-N retention cohort analysis."""
        key = f"analytics:retention:{tenant_id}:{cohort_event}:{return_event}:{start.date()}:{end.date()}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                WITH cohort AS (
                    SELECT user_id, date_trunc('day', MIN(received_at)) AS cohort_day
                    FROM events
                    WHERE tenant_id = :tid
                      AND event_type = :cohort_event
                      AND received_at BETWEEN :start AND :end
                      AND user_id IS NOT NULL
                    GROUP BY user_id
                ),
                activity AS (
                    SELECT e.user_id,
                           date_trunc('day', e.received_at) AS activity_day,
                           c.cohort_day
                    FROM events e
                    JOIN cohort c ON e.user_id = c.user_id
                    WHERE e.tenant_id = :tid
                      AND e.event_type = :return_event
                      AND e.received_at BETWEEN :start AND :end
                )
                SELECT
                    cohort_day,
                    (activity_day - cohort_day) AS day_number,
                    COUNT(DISTINCT user_id)      AS retained_users,
                    (SELECT COUNT(*) FROM cohort WHERE cohort_day = a.cohort_day) AS cohort_size
                FROM activity a
                GROUP BY cohort_day, day_number
                ORDER BY cohort_day, day_number
            """),
            {
                "tid": str(tenant_id),
                "cohort_event": cohort_event,
                "return_event": return_event,
                "start": start,
                "end": end,
            },
        )
        data = [
            {
                "cohort_day": row["cohort_day"].isoformat(),
                "day_number": int(row["day_number"].days),
                "retained_users": int(row["retained_users"]),
                "cohort_size": int(row["cohort_size"]),
                "retention_rate": round(row["retained_users"] / row["cohort_size"] * 100, 2),
            }
            for row in result.mappings()
        ]
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data


analytics_service = AnalyticsService()
