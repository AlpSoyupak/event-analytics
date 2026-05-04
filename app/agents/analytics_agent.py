import json
import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.analytics_service import analytics_service
from app.services.llm_service import llm_service

MAX_ROUNDS = 6

SYSTEM_PROMPT = """\
You are an analytics assistant for an event tracking platform.
You have access to tools that query real event data for the current tenant.
Use them to answer questions accurately. Today's date is {today}.

When asked about trends, anomalies, or recommendations:
1. Fetch the relevant data using the available tools.
2. Reason over the numbers before answering.
3. Provide a concise, insightful answer that cites specific figures.

Available event types in this platform: page_view, button_click, form_submit, purchase,
signup, login, logout, search, add_to_cart, checkout.
"""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_summary",
            "description": (
                "Get aggregate event counts, unique users, unique sessions, and a breakdown "
                "by event type for a given time window."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "start": {"type": "string", "description": "ISO 8601 start datetime, e.g. 2026-04-01T00:00:00Z"},
                    "end": {"type": "string", "description": "ISO 8601 end datetime, e.g. 2026-04-30T23:59:59Z"},
                },
                "required": ["start", "end"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_timeseries",
            "description": "Get event counts over time bucketed by hour, day, or week. Useful for spotting trends and drops.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start": {"type": "string", "description": "ISO 8601 start datetime"},
                    "end": {"type": "string", "description": "ISO 8601 end datetime"},
                    "granularity": {
                        "type": "string",
                        "enum": ["hour", "day", "week"],
                        "description": "Time bucket size",
                    },
                    "event_type": {
                        "type": "string",
                        "description": "Filter to a specific event type (optional). Omit to get all events.",
                    },
                },
                "required": ["start", "end", "granularity"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_events",
            "description": "Get the most frequent event types ranked by count, with their unique user counts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start": {"type": "string", "description": "ISO 8601 start datetime"},
                    "end": {"type": "string", "description": "ISO 8601 end datetime"},
                    "limit": {
                        "type": "integer",
                        "description": "Number of top events to return (1-100), default 10.",
                        "default": 10,
                    },
                },
                "required": ["start", "end"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_funnel",
            "description": (
                "Analyze sequential conversion through an ordered series of event types. "
                "Shows how many users completed each step."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "steps": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Ordered list of 2-10 event types, "
                            "e.g. ['page_view', 'add_to_cart', 'purchase']"
                        ),
                    },
                    "start": {"type": "string", "description": "ISO 8601 start datetime"},
                    "end": {"type": "string", "description": "ISO 8601 end datetime"},
                },
                "required": ["steps", "start", "end"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_retention",
            "description": (
                "Day-N cohort retention: tracks users who performed a cohort event "
                "and measures how many returned to do a return event on each subsequent day."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "cohort_event": {
                        "type": "string",
                        "description": "Event that defines cohort entry, e.g. 'signup'",
                    },
                    "return_event": {
                        "type": "string",
                        "description": "Event tracked for retention, e.g. 'login'",
                    },
                    "start": {"type": "string", "description": "ISO 8601 start datetime"},
                    "end": {"type": "string", "description": "ISO 8601 end datetime"},
                },
                "required": ["cohort_event", "return_event", "start", "end"],
            },
        },
    },
]


def _parse_dt(s: str) -> datetime:
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


class AnalyticsAgent:
    def __init__(self, tenant_id: uuid.UUID, db: AsyncSession):
        self.tenant_id = tenant_id
        self.db = db

    async def _execute_tool(self, name: str, args: dict) -> str:
        try:
            if name == "get_summary":
                result = await analytics_service.get_summary(
                    self.tenant_id, _parse_dt(args["start"]), _parse_dt(args["end"]), self.db
                )
            elif name == "get_timeseries":
                result = await analytics_service.get_timeseries(
                    self.tenant_id,
                    _parse_dt(args["start"]),
                    _parse_dt(args["end"]),
                    args.get("granularity", "day"),
                    self.db,
                    args.get("event_type"),
                )
            elif name == "get_top_events":
                result = await analytics_service.get_top_events(
                    self.tenant_id,
                    _parse_dt(args["start"]),
                    _parse_dt(args["end"]),
                    args.get("limit", 10),
                    self.db,
                )
            elif name == "get_funnel":
                result = await analytics_service.get_funnel(
                    self.tenant_id,
                    args["steps"],
                    _parse_dt(args["start"]),
                    _parse_dt(args["end"]),
                    self.db,
                )
            elif name == "get_retention":
                result = await analytics_service.get_retention(
                    self.tenant_id,
                    _parse_dt(args["start"]),
                    _parse_dt(args["end"]),
                    args["cohort_event"],
                    args["return_event"],
                    self.db,
                )
            else:
                return json.dumps({"error": f"Unknown tool: {name}"})
            return json.dumps(result, default=str)
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    async def run(self, question: str) -> tuple[str, list[str]]:
        """Run the agentic tool loop.

        Returns (answer, tools_used) so callers can inspect which tools fired.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        messages: list[dict] = [
            {"role": "system", "content": SYSTEM_PROMPT.format(today=today)},
            {"role": "user", "content": question},
        ]
        tools_used: list[str] = []

        for _ in range(MAX_ROUNDS):
            response = await llm_service.chat_with_tools(messages, TOOLS)

            if not response.tool_calls:
                return response.content or "", tools_used

            # Append the assistant turn (must include tool_calls for the API)
            messages.append({
                "role": "assistant",
                "content": response.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in response.tool_calls
                ],
            })

            # Execute every tool call and feed results back
            for tc in response.tool_calls:
                tools_used.append(tc.function.name)
                args = json.loads(tc.function.arguments)
                tool_result = await self._execute_tool(tc.function.name, args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tool_result,
                })

        # Exceeded MAX_ROUNDS — ask for a final synthesis with data already in context
        messages.append({
            "role": "user",
            "content": "Please provide your final answer based on the data retrieved so far.",
        })
        return await llm_service.chat(messages), tools_used
