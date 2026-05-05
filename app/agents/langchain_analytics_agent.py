"""LangChain analytics agent using tool-calling and LCEL.

Provides the same (answer, tools_used) interface as AnalyticsAgent but uses:
- ChatGroq via langchain-groq
- LangChain StructuredTool wrappers around analytics_service functions
- create_tool_calling_agent + AgentExecutor (LCEL under the hood)
"""
import json
import uuid
from datetime import datetime, timezone

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import StructuredTool
from langchain_groq import ChatGroq
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.services.analytics_service import analytics_service

settings = get_settings()

_SYSTEM = """\
You are an analytics assistant for an event tracking platform.
Use the provided tools to query real event data and answer questions accurately.
Today's date is {today}.

When asked about trends, anomalies, or recommendations:
1. Fetch relevant data using the tools.
2. Reason over the numbers before answering.
3. Provide a concise answer that cites specific figures.

Available event types: page_view, button_click, form_submit, purchase,
signup, login, logout, search, add_to_cart, checkout."""

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", _SYSTEM),
    ("human", "{question}"),
    MessagesPlaceholder("agent_scratchpad"),
])


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


# ── Agent ─────────────────────────────────────────────────────────────────────

class LangChainAnalyticsAgent:
    """Analytics agent built on LangChain tool-calling and LCEL.

    Wraps analytics_service functions as LangChain StructuredTools bound to the
    current tenant/session, then runs them through ChatGroq via AgentExecutor.
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
        ]

    async def run(self, question: str) -> tuple[str, list[str]]:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        tools = self._build_tools()

        llm = ChatGroq(
            groq_api_key=settings.groq_api_key,
            model=settings.groq_model,
        )

        agent = create_tool_calling_agent(llm, tools, _PROMPT)
        executor = AgentExecutor(agent=agent, tools=tools, max_iterations=6)

        result = await executor.ainvoke({"question": question, "today": today})

        tools_used = [
            step[0].tool
            for step in result.get("intermediate_steps", [])
            if hasattr(step[0], "tool")
        ]
        return result.get("output", ""), tools_used
