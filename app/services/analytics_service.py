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


    async def get_customer_segments(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        db: AsyncSession,
    ) -> list[dict]:
        """Segments users by purchase frequency: one_time / repeat / loyal."""
        key = f"analytics:customers:{tenant_id}:{start.date()}:{end.date()}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                WITH user_stats AS (
                    SELECT
                        user_id,
                        COUNT(DISTINCT properties->>'order_id')          AS order_count,
                        SUM((properties->>'value')::numeric)             AS total_spend,
                        MIN(received_at)                                 AS first_purchase,
                        MAX(received_at)                                 AS last_purchase
                    FROM events
                    WHERE tenant_id = :tid
                      AND event_type = 'purchase'
                      AND received_at BETWEEN :start AND :end
                      AND user_id IS NOT NULL
                      AND properties->>'value' IS NOT NULL
                    GROUP BY user_id
                ),
                segmented AS (
                    SELECT *,
                        CASE
                            WHEN order_count = 1           THEN 'one_time'
                            WHEN order_count BETWEEN 2 AND 4 THEN 'repeat'
                            ELSE                                'loyal'
                        END AS segment
                    FROM user_stats
                )
                SELECT
                    segment,
                    COUNT(*)                                              AS user_count,
                    ROUND(AVG(order_count), 1)                           AS avg_orders,
                    ROUND(AVG(total_spend)::numeric, 2)                  AS avg_ltv,
                    ROUND(SUM(total_spend)::numeric, 2)                  AS total_revenue,
                    ROUND(AVG(
                        EXTRACT(EPOCH FROM (last_purchase - first_purchase)) / 86400
                    )::numeric, 1)                                       AS avg_days_active
                FROM segmented
                GROUP BY segment
                ORDER BY avg_ltv DESC
            """),
            {"tid": str(tenant_id), "start": start, "end": end},
        )
        data = [
            {
                "segment":        row["segment"],
                "user_count":     int(row["user_count"]),
                "avg_orders":     float(row["avg_orders"] or 0),
                "avg_ltv":        float(row["avg_ltv"] or 0),
                "total_revenue":  float(row["total_revenue"] or 0),
                "avg_days_active": float(row["avg_days_active"] or 0),
            }
            for row in result.mappings()
        ]
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_search_gaps(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        limit: int,
        db: AsyncSession,
    ) -> list[dict]:
        """Search queries ranked by zero-result frequency — catalog gap signals."""
        key = f"analytics:searchgaps:{tenant_id}:{start.date()}:{end.date()}:{limit}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                SELECT
                    properties->>'query'                                  AS query,
                    COUNT(*)                                              AS search_count,
                    ROUND(AVG((properties->>'results_count')::int), 1)   AS avg_results,
                    SUM(CASE WHEN (properties->>'results_count')::int = 0
                             THEN 1 ELSE 0 END)                          AS zero_result_count,
                    ROUND(
                        SUM(CASE WHEN (properties->>'results_count')::int = 0 THEN 1 ELSE 0 END)::numeric
                        / COUNT(*) * 100, 1
                    )                                                     AS zero_result_pct
                FROM events
                WHERE tenant_id = :tid
                  AND event_type = 'search'
                  AND received_at BETWEEN :start AND :end
                  AND properties->>'query' IS NOT NULL
                  AND properties->>'results_count' IS NOT NULL
                GROUP BY properties->>'query'
                ORDER BY zero_result_count DESC, search_count DESC
                LIMIT :lim
            """),
            {"tid": str(tenant_id), "start": start, "end": end, "lim": limit},
        )
        data = [
            {
                "query":            row["query"],
                "search_count":     int(row["search_count"]),
                "avg_results":      float(row["avg_results"] or 0),
                "zero_result_count": int(row["zero_result_count"]),
                "zero_result_pct":  float(row["zero_result_pct"] or 0),
            }
            for row in result.mappings()
        ]
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_basket_analysis(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        limit: int,
        db: AsyncSession,
    ) -> list[dict]:
        """Top co-purchased product pairs (basket analysis)."""
        key = f"analytics:basket:{tenant_id}:{start.date()}:{end.date()}:{limit}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                WITH basket_items AS (
                    SELECT
                        properties->>'order_id'    AS order_id,
                        properties->>'product_id'  AS product_id,
                        MAX(properties->>'product_name') AS product_name
                    FROM events
                    WHERE tenant_id = :tid
                      AND event_type = 'purchase'
                      AND received_at BETWEEN :start AND :end
                      AND properties->>'order_id'   IS NOT NULL
                      AND properties->>'product_id' IS NOT NULL
                    GROUP BY properties->>'order_id', properties->>'product_id'
                ),
                pairs AS (
                    SELECT
                        a.product_id   AS product_a_id,
                        a.product_name AS product_a_name,
                        b.product_id   AS product_b_id,
                        b.product_name AS product_b_name,
                        COUNT(*)       AS co_purchase_count
                    FROM basket_items a
                    JOIN basket_items b
                      ON a.order_id = b.order_id
                     AND a.product_id < b.product_id
                    GROUP BY a.product_id, a.product_name, b.product_id, b.product_name
                ),
                totals AS (
                    SELECT product_id, COUNT(DISTINCT order_id) AS total_orders
                    FROM basket_items GROUP BY product_id
                )
                SELECT
                    p.product_a_id,   p.product_a_name,
                    p.product_b_id,   p.product_b_name,
                    p.co_purchase_count,
                    ROUND(p.co_purchase_count::numeric / NULLIF(ta.total_orders, 0) * 100, 1) AS attach_rate_a,
                    ROUND(p.co_purchase_count::numeric / NULLIF(tb.total_orders, 0) * 100, 1) AS attach_rate_b
                FROM pairs p
                JOIN totals ta ON ta.product_id = p.product_a_id
                JOIN totals tb ON tb.product_id = p.product_b_id
                ORDER BY co_purchase_count DESC
                LIMIT :lim
            """),
            {"tid": str(tenant_id), "start": start, "end": end, "lim": limit},
        )
        data = [
            {
                "product_a_id":       row["product_a_id"],
                "product_a_name":     row["product_a_name"],
                "product_b_id":       row["product_b_id"],
                "product_b_name":     row["product_b_name"],
                "co_purchase_count":  int(row["co_purchase_count"]),
                "attach_rate_a":      float(row["attach_rate_a"] or 0),
                "attach_rate_b":      float(row["attach_rate_b"] or 0),
            }
            for row in result.mappings()
        ]
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_selling_times(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        db: AsyncSession,
    ) -> dict:
        """Purchase volume and revenue by hour-of-day and day-of-week."""
        key = f"analytics:sellingtimes:{tenant_id}:{start.date()}:{end.date()}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                SELECT
                    EXTRACT(hour FROM received_at)::int  AS hour_of_day,
                    EXTRACT(dow  FROM received_at)::int  AS day_of_week,
                    COUNT(*)                             AS purchase_count,
                    SUM((properties->>'value')::numeric) AS revenue
                FROM events
                WHERE tenant_id = :tid
                  AND event_type = 'purchase'
                  AND received_at BETWEEN :start AND :end
                  AND properties->>'value' IS NOT NULL
                GROUP BY hour_of_day, day_of_week
                ORDER BY day_of_week, hour_of_day
            """),
            {"tid": str(tenant_id), "start": start, "end": end},
        )
        rows = list(result.mappings())

        # Aggregate to two views: by_hour and by_day
        by_hour: dict[int, dict] = {h: {"purchase_count": 0, "revenue": 0.0} for h in range(24)}
        by_day:  dict[int, dict] = {d: {"purchase_count": 0, "revenue": 0.0} for d in range(7)}
        day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

        for row in rows:
            h, d = int(row["hour_of_day"]), int(row["day_of_week"])
            by_hour[h]["purchase_count"] += int(row["purchase_count"])
            by_hour[h]["revenue"]        += float(row["revenue"] or 0)
            by_day[d]["purchase_count"]  += int(row["purchase_count"])
            by_day[d]["revenue"]         += float(row["revenue"] or 0)

        peak_hour = max(by_hour, key=lambda h: by_hour[h]["revenue"])
        peak_day  = max(by_day,  key=lambda d: by_day[d]["revenue"])

        data = {
            "peak_hour":     peak_hour,
            "peak_day":      day_names[peak_day],
            "by_hour":       [{"hour": h, **v} for h, v in by_hour.items()],
            "by_day":        [{"day": day_names[d], **v} for d, v in by_day.items()],
        }
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_traffic_sources(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        db: AsyncSession,
    ) -> list[dict]:
        """Per-product conversion breakdown by traffic source (referrer)."""
        key = f"analytics:traffic:{tenant_id}:{start.date()}:{end.date()}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                SELECT
                    properties->>'product_id'                             AS product_id,
                    MAX(properties->>'product_name')                      AS product_name,
                    properties->>'referrer'                               AS referrer,
                    SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END)  AS views,
                    SUM(CASE WHEN event_type = 'add_to_cart'  THEN 1 ELSE 0 END)  AS cart_adds,
                    SUM(CASE WHEN event_type = 'purchase'     THEN 1 ELSE 0 END)  AS purchases
                FROM events
                WHERE tenant_id = :tid
                  AND event_type IN ('product_view', 'add_to_cart', 'purchase')
                  AND received_at BETWEEN :start AND :end
                  AND properties->>'referrer'    IS NOT NULL
                  AND properties->>'product_id'  IS NOT NULL
                GROUP BY properties->>'product_id', properties->>'referrer'
                ORDER BY product_id, purchases DESC
            """),
            {"tid": str(tenant_id), "start": start, "end": end},
        )
        rows = list(result.mappings())
        data = [
            {
                "product_id":            row["product_id"],
                "product_name":          row["product_name"],
                "referrer":              row["referrer"],
                "views":                 int(row["views"] or 0),
                "cart_adds":             int(row["cart_adds"] or 0),
                "purchases":             int(row["purchases"] or 0),
                "view_to_purchase_rate": round(int(row["purchases"] or 0) / int(row["views"]) * 100, 1)
                                         if int(row["views"] or 0) else 0.0,
            }
            for row in rows
        ]
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_experiment_results(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        db: AsyncSession,
    ) -> list[dict]:
        """A/B experiment results: per-variant views, conversions, revenue, and uplift."""
        key = f"analytics:experiments:{tenant_id}:{start.date()}:{end.date()}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                SELECT
                    properties->>'experiment_id'   AS experiment_id,
                    MAX(properties->>'experiment_name') AS experiment_name,
                    properties->>'variant'         AS variant,
                    SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END)  AS views,
                    SUM(CASE WHEN event_type = 'add_to_cart'  THEN 1 ELSE 0 END)  AS cart_adds,
                    SUM(CASE WHEN event_type = 'purchase'     THEN 1 ELSE 0 END)  AS purchases,
                    SUM(CASE WHEN event_type = 'purchase'
                             THEN (properties->>'value')::numeric ELSE 0 END)     AS revenue
                FROM events
                WHERE tenant_id = :tid
                  AND event_type IN ('product_view', 'add_to_cart', 'purchase')
                  AND received_at BETWEEN :start AND :end
                  AND properties->>'experiment_id' IS NOT NULL
                GROUP BY properties->>'experiment_id', properties->>'variant'
                ORDER BY experiment_id, variant
            """),
            {"tid": str(tenant_id), "start": start, "end": end},
        )
        rows = list(result.mappings())

        # Group rows by experiment, then compute uplift relative to control
        from collections import defaultdict
        by_exp: dict = defaultdict(list)
        for row in rows:
            views = int(row["views"] or 0)
            purchases = int(row["purchases"] or 0)
            by_exp[row["experiment_id"]].append({
                "variant":               row["variant"],
                "views":                 views,
                "cart_adds":             int(row["cart_adds"] or 0),
                "purchases":             purchases,
                "revenue":               round(float(row["revenue"] or 0), 2),
                "view_to_purchase_rate": round(purchases / views * 100, 2) if views else 0.0,
            })

        data = []
        for exp_id, variants in by_exp.items():
            exp_name = rows[[r["experiment_id"] for r in rows].index(exp_id)]["experiment_name"]
            # Find control conversion rate for uplift baseline
            control = next((v for v in variants if "original" in v["variant"] or "no_" in v["variant"] or "control" in v["variant"]), variants[0])
            control_rate = control["view_to_purchase_rate"]

            variants_with_uplift = []
            for v in variants:
                uplift = round((v["view_to_purchase_rate"] - control_rate) / control_rate * 100, 1) if control_rate else 0.0
                variants_with_uplift.append({**v, "uplift_vs_control_pct": uplift})

            data.append({
                "experiment_id":   exp_id,
                "experiment_name": exp_name,
                "variants":        variants_with_uplift,
            })

        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_revenue(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        db: AsyncSession,
    ) -> dict:
        key = f"analytics:revenue:{tenant_id}:{start.date()}:{end.date()}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                SELECT
                    DATE_TRUNC('day', received_at)                        AS day,
                    COUNT(*)                                              AS order_count,
                    SUM((properties->>'value')::numeric)                  AS revenue,
                    COUNT(DISTINCT user_id)                               AS unique_buyers
                FROM events
                WHERE tenant_id = :tid
                  AND event_type = 'purchase'
                  AND received_at BETWEEN :start AND :end
                  AND properties->>'value' IS NOT NULL
                GROUP BY day
                ORDER BY day
            """),
            {"tid": str(tenant_id), "start": start, "end": end},
        )
        daily = [
            {
                "date": row["day"].date().isoformat(),
                "revenue": float(row["revenue"] or 0),
                "order_count": int(row["order_count"]),
                "unique_buyers": int(row["unique_buyers"]),
            }
            for row in result.mappings()
        ]

        total_revenue = sum(d["revenue"] for d in daily)
        total_orders = sum(d["order_count"] for d in daily)

        # Revenue by category
        cat_result = await db.execute(
            text("""
                SELECT
                    properties->>'category'                               AS category,
                    COUNT(*)                                              AS order_count,
                    SUM((properties->>'value')::numeric)                  AS revenue
                FROM events
                WHERE tenant_id = :tid
                  AND event_type = 'purchase'
                  AND received_at BETWEEN :start AND :end
                  AND properties->>'category' IS NOT NULL
                GROUP BY category
                ORDER BY revenue DESC
            """),
            {"tid": str(tenant_id), "start": start, "end": end},
        )
        by_category = [
            {
                "category": row["category"],
                "revenue": float(row["revenue"] or 0),
                "order_count": int(row["order_count"]),
                "share": round(float(row["revenue"] or 0) / total_revenue * 100, 1) if total_revenue else 0.0,
            }
            for row in cat_result.mappings()
        ]

        data = {
            "tenant_id": str(tenant_id),
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "total_revenue": round(total_revenue, 2),
            "total_orders": total_orders,
            "avg_order_value": round(total_revenue / total_orders, 2) if total_orders else 0.0,
            "daily": daily,
            "by_category": by_category,
        }
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data

    async def get_product_performance(
        self,
        tenant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        limit: int,
        db: AsyncSession,
    ) -> list[dict]:
        key = f"analytics:products:{tenant_id}:{start.date()}:{end.date()}:{limit}"
        if cached := await cache_service.get(key):
            return cached

        result = await db.execute(
            text("""
                SELECT
                    properties->>'product_id'                             AS product_id,
                    MAX(properties->>'product_name')                      AS product_name,
                    MAX(properties->>'category')                          AS category,
                    SUM(CASE WHEN event_type = 'product_view'  THEN 1 ELSE 0 END)                           AS views,
                    SUM(CASE WHEN event_type = 'add_to_cart'   THEN 1 ELSE 0 END)                           AS cart_adds,
                    SUM(CASE WHEN event_type = 'purchase'      THEN 1 ELSE 0 END)                           AS purchases,
                    SUM(CASE WHEN event_type = 'purchase'
                             THEN (properties->>'value')::numeric ELSE 0 END)                              AS revenue
                FROM events
                WHERE tenant_id = :tid
                  AND event_type IN ('product_view', 'add_to_cart', 'purchase')
                  AND received_at BETWEEN :start AND :end
                  AND properties->>'product_id' IS NOT NULL
                GROUP BY properties->>'product_id'
                ORDER BY revenue DESC
                LIMIT :lim
            """),
            {"tid": str(tenant_id), "start": start, "end": end, "lim": limit},
        )
        rows = list(result.mappings())
        data = [
            {
                "product_id":        row["product_id"],
                "product_name":      row["product_name"],
                "category":          row["category"],
                "views":             int(row["views"] or 0),
                "cart_adds":         int(row["cart_adds"] or 0),
                "purchases":         int(row["purchases"] or 0),
                "revenue":           round(float(row["revenue"] or 0), 2),
                "view_to_cart_rate": round(int(row["cart_adds"] or 0) / int(row["views"]) * 100, 1) if int(row["views"] or 0) else 0.0,
                "cart_to_purchase_rate": round(int(row["purchases"] or 0) / int(row["cart_adds"]) * 100, 1) if int(row["cart_adds"] or 0) else 0.0,
                "view_to_purchase_rate": round(int(row["purchases"] or 0) / int(row["views"]) * 100, 1) if int(row["views"] or 0) else 0.0,
            }
            for row in rows
        ]
        await cache_service.set(key, data, ttl=settings.analytics_cache_ttl)
        return data


analytics_service = AnalyticsService()
