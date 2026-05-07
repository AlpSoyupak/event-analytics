"""LangChain analytics agent using LangGraph's create_react_agent.

AgentExecutor was removed in LangChain 1.x. This uses the modern replacement:
- ChatGroq via langchain-groq
- LangChain StructuredTool wrappers around analytics_service functions
- LangGraph create_react_agent (the current recommended agentic loop)
"""
import json
import uuid
from datetime import datetime, timezone

from langchain_core.messages import SystemMessage, ToolMessage
from langchain_core.tools import StructuredTool
from langchain_groq import ChatGroq
from langgraph.prebuilt import create_react_agent
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.services.analytics_service import analytics_service

settings = get_settings()

_SYSTEM = """\
You are an e-commerce analytics assistant. Use the provided tools to query \
real event data and answer business questions accurately.
Today's date is {today}.

When asked about trends, anomalies, or recommendations:
1. Fetch relevant data using the tools.
2. Reason over the numbers before answering.
3. Provide a concise answer that cites specific figures.
4. When asked "how to sell more", always check product conversion rates and \
   identify products with high views but low cart or purchase rates — those are \
   the biggest improvement opportunities.

## Event taxonomy
| Event          | Meaning                              | Key properties                                      |
|----------------|--------------------------------------|-----------------------------------------------------|
| page_view      | User visited a page                  | page                                                |
| product_view   | User viewed a product detail page    | product_id, product_name, category, price           |
| add_to_cart    | User added a product to their cart   | product_id, product_name, category, price, quantity, value |
| checkout       | User reached the checkout step       | value (cart total), items (count)                   |
| purchase       | User completed a purchase            | product_id, product_name, category, price, quantity, value |
| search         | User searched for something          | query                                               |
| signup / login / logout | Auth events                 | —                                                   |

## Conversion funnel
page_view → product_view → add_to_cart → checkout → purchase

## Level 2 — Traffic source segmentation
product_view, add_to_cart, and purchase events carry a `referrer` field:
  search | homepage_banner | category_page | email_campaign | deals_page
product_view also carries `search_query` (when referrer=search) and `time_on_page` (seconds).
Use get_traffic_sources to compare conversion rates across referrers for each product.
A product with low overall conversion that converts well from search but poorly from
homepage_banner has a traffic-quality problem, not a product problem.

## Level 3 — Controlled A/B experiments
experiment events carry `experiment_id`, `experiment_name`, and `variant` fields.
Active experiments:
  - exp_001 "smart_watch_price_discount": control=original_price vs treatment=discount_10pct
  - exp_002 "headphones_social_proof":   control=no_reviews    vs treatment=show_47_reviews
Use get_experiment_results to compare control vs treatment conversion rates and revenue uplift.
`uplift_vs_control_pct` is the % change in view-to-purchase rate vs the control group.
This is the only place in the data where you can make causal claims ("doing X caused Y% more sales").

## Answering "how to sell more" (product performance)
1. get_product_performance — find products with high views but low view_to_purchase_rate
2. get_traffic_sources — is it a traffic-quality problem or a product problem?
3. get_experiment_results — which interventions have proven uplift?

## Answering "who are my customers / churn / LTV"
→ get_customer_segments — one_time vs repeat vs loyal, with avg_ltv and total_revenue

## Answering "what should I add to my catalog"
→ get_search_gaps — queries with high zero_result_pct are unmet demand signals

## Answering "what should I bundle / recommend together"
→ get_basket_analysis — co_purchase_count and attach_rate between product pairs
   attach_rate_a = % of orders containing A that also contained B

## Answering "when should I run promotions / send emails"
→ get_selling_times — peak_hour and peak_day, plus full by_hour and by_day breakdowns

## Causal vs. correlational claims
Only cite causal claims ("doing X caused Y% more sales") when using get_experiment_results.
All other tools are correlational — describe them as patterns, not causes."""


def _dt(s: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ── Pydantic schemas for tool inputs ─────────────────────────────────────────

class _SummaryIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime, e.g. 2026-04-01T00:00:00Z")
    end: str = Field(description="ISO 8601 end datetime")


class _TimeseriesIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")
    granularity: str = Field(default="day", description="hour | day | week")
    event_type: str = Field(default="", description="Filter to a specific event type (optional)")


class _TopEventsIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")
    limit: int = Field(default=10, description="Number of top events to return (1–100)")


class _FunnelIn(BaseModel):
    steps: list[str] = Field(
        description="Ordered list of event types, e.g. ['signup', 'login', 'purchase']"
    )
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")


class _RetentionIn(BaseModel):
    cohort_event: str = Field(description="Event defining cohort entry, e.g. 'signup'")
    return_event: str = Field(description="Event tracked for retention, e.g. 'login'")
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")


class _RevenueIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")


class _ProductsIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")
    limit: int = Field(default=20, description="Max number of products to return (1–100)")


class _TrafficSourcesIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")


class _ExperimentsIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")


class _CustomerSegmentsIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")


class _SearchGapsIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")
    limit: int = Field(default=20, description="Max queries to return")


class _BasketIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")
    limit: int = Field(default=20, description="Max product pairs to return")


class _SellingTimesIn(BaseModel):
    start: str = Field(description="ISO 8601 start datetime")
    end: str = Field(description="ISO 8601 end datetime")


# ── Agent ─────────────────────────────────────────────────────────────────────

class LangChainAnalyticsAgent:
    """Analytics agent built on LangGraph's create_react_agent.

    Wraps analytics_service functions as LangChain StructuredTools bound to the
    current tenant/session, then drives the tool-calling loop via LangGraph.
    """

    def __init__(self, tenant_id: uuid.UUID, db: AsyncSession) -> None:
        self.tenant_id = tenant_id
        self.db = db

    def _build_tools(self) -> list[StructuredTool]:
        tid, db = self.tenant_id, self.db

        async def get_summary(start: str, end: str) -> str:
            r = await analytics_service.get_summary(tid, _dt(start), _dt(end), db)
            return json.dumps(r, default=str)

        async def get_timeseries(
            start: str, end: str, granularity: str = "day", event_type: str = ""
        ) -> str:
            r = await analytics_service.get_timeseries(
                tid, _dt(start), _dt(end), granularity, db, event_type or None
            )
            return json.dumps(r, default=str)

        async def get_top_events(start: str, end: str, limit: int = 10) -> str:
            r = await analytics_service.get_top_events(tid, _dt(start), _dt(end), limit, db)
            return json.dumps(r, default=str)

        async def get_funnel(steps: list[str], start: str, end: str) -> str:
            r = await analytics_service.get_funnel(tid, steps, _dt(start), _dt(end), db)
            return json.dumps(r, default=str)

        async def get_retention(
            cohort_event: str, return_event: str, start: str, end: str
        ) -> str:
            r = await analytics_service.get_retention(
                tid, _dt(start), _dt(end), cohort_event, return_event, db
            )
            return json.dumps(r, default=str)

        async def get_revenue(start: str, end: str) -> str:
            r = await analytics_service.get_revenue(tid, _dt(start), _dt(end), db)
            return json.dumps(r, default=str)

        async def get_product_performance(start: str, end: str, limit: int = 20) -> str:
            r = await analytics_service.get_product_performance(tid, _dt(start), _dt(end), limit, db)
            return json.dumps(r, default=str)

        async def get_traffic_sources(start: str, end: str) -> str:
            r = await analytics_service.get_traffic_sources(tid, _dt(start), _dt(end), db)
            return json.dumps(r, default=str)

        async def get_experiment_results(start: str, end: str) -> str:
            r = await analytics_service.get_experiment_results(tid, _dt(start), _dt(end), db)
            return json.dumps(r, default=str)

        async def get_customer_segments(start: str, end: str) -> str:
            r = await analytics_service.get_customer_segments(tid, _dt(start), _dt(end), db)
            return json.dumps(r, default=str)

        async def get_search_gaps(start: str, end: str, limit: int = 20) -> str:
            r = await analytics_service.get_search_gaps(tid, _dt(start), _dt(end), limit, db)
            return json.dumps(r, default=str)

        async def get_basket_analysis(start: str, end: str, limit: int = 20) -> str:
            r = await analytics_service.get_basket_analysis(tid, _dt(start), _dt(end), limit, db)
            return json.dumps(r, default=str)

        async def get_selling_times(start: str, end: str) -> str:
            r = await analytics_service.get_selling_times(tid, _dt(start), _dt(end), db)
            return json.dumps(r, default=str)

        return [
            StructuredTool.from_function(
                coroutine=get_summary,
                name="get_summary",
                args_schema=_SummaryIn,
                description="Get aggregate event counts, unique users, unique sessions, and event-type breakdown for a time window.",
            ),
            StructuredTool.from_function(
                coroutine=get_timeseries,
                name="get_timeseries",
                args_schema=_TimeseriesIn,
                description="Get event counts over time bucketed by hour, day, or week. Useful for spotting trends and drops.",
            ),
            StructuredTool.from_function(
                coroutine=get_top_events,
                name="get_top_events",
                args_schema=_TopEventsIn,
                description="Get the most frequent event types ranked by count, with unique user counts.",
            ),
            StructuredTool.from_function(
                coroutine=get_funnel,
                name="get_funnel",
                args_schema=_FunnelIn,
                description="Analyze sequential conversion through an ordered series of event types.",
            ),
            StructuredTool.from_function(
                coroutine=get_retention,
                name="get_retention",
                args_schema=_RetentionIn,
                description="Day-N cohort retention: tracks users who did the cohort event and measures return rate on subsequent days.",
            ),
            StructuredTool.from_function(
                coroutine=get_revenue,
                name="get_revenue",
                args_schema=_RevenueIn,
                description="Get total revenue, daily revenue breakdown, average order value, and revenue split by product category. Use this for all revenue and sales questions.",
            ),
            StructuredTool.from_function(
                coroutine=get_product_performance,
                name="get_product_performance",
                args_schema=_ProductsIn,
                description="Get per-product metrics: views, cart-adds, purchases, revenue, and conversion rates (view→cart, cart→purchase, view→purchase). Use this to identify which products have the highest drop-off and to answer 'how do I sell more' questions.",
            ),
            StructuredTool.from_function(
                coroutine=get_traffic_sources,
                name="get_traffic_sources",
                args_schema=_TrafficSourcesIn,
                description="Per-product conversion rates broken down by traffic source (referrer: search, homepage_banner, category_page, email_campaign, deals_page). Use this to distinguish traffic-quality problems from product problems. A product that converts well from search but poorly from homepage_banner needs better targeting, not a price change.",
            ),
            StructuredTool.from_function(
                coroutine=get_experiment_results,
                name="get_experiment_results",
                args_schema=_ExperimentsIn,
                description="A/B experiment results with control vs treatment conversion rates, revenue, and uplift_vs_control_pct. This is the only data that supports causal claims. Use this to recommend proven interventions with quantified impact.",
            ),
            StructuredTool.from_function(
                coroutine=get_customer_segments,
                name="get_customer_segments",
                args_schema=_CustomerSegmentsIn,
                description="Segments users into one_time (1 order), repeat (2–4 orders), and loyal (5+ orders). Returns user_count, avg_orders, avg_ltv, total_revenue, avg_days_active per segment. Use for churn questions, LTV analysis, and 'who are my best customers' questions.",
            ),
            StructuredTool.from_function(
                coroutine=get_search_gaps,
                name="get_search_gaps",
                args_schema=_SearchGapsIn,
                description="Search queries ranked by zero_result_count and zero_result_pct. High-volume queries with zero results are catalog gaps — products users want that you don't carry. Use for 'what should I add to my catalog' or 'what are users trying to find' questions.",
            ),
            StructuredTool.from_function(
                coroutine=get_basket_analysis,
                name="get_basket_analysis",
                args_schema=_BasketIn,
                description="Top co-purchased product pairs with co_purchase_count and attach rates (% of orders for product A that also included product B). Use for cross-sell recommendations and 'what should I bundle together' questions.",
            ),
            StructuredTool.from_function(
                coroutine=get_selling_times,
                name="get_selling_times",
                args_schema=_SellingTimesIn,
                description="Purchase volume and revenue broken down by hour_of_day (0–23) and day_of_week. Returns peak_hour and peak_day. Use for 'when should I run promotions', 'what is my busiest time', or 'when should I send marketing emails' questions.",
            ),
        ]

    async def run(self, question: str) -> tuple[str, list[str]]:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        tools = self._build_tools()

        llm = ChatGroq(
            groq_api_key=settings.groq_api_key,
            model=settings.groq_model,
        )

        graph = create_react_agent(
            llm,
            tools,
            prompt=SystemMessage(content=_SYSTEM.format(today=today)),
        )

        result = await graph.ainvoke({
            "messages": [{"role": "human", "content": question}]
        })

        # Final AI message is always last
        answer = result["messages"][-1].content

        # ToolMessages carry the name of the tool that was called
        tools_used = [
            msg.name
            for msg in result["messages"]
            if isinstance(msg, ToolMessage)
        ]

        return answer, tools_used
