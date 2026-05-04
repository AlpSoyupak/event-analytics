from datetime import datetime, timezone

from app.services.llm_service import llm_service
from app.services.rag_service import rag_service

PLANNER_SYSTEM = """\
You are a query planner for an analytics assistant.

Your job is NOT to answer the question — only to plan which analytics tools should \
be called and in what order.

Available tools:
- get_summary(start, end): aggregate counts, unique users, event-type breakdown
- get_timeseries(start, end, granularity, event_type?): time-bucketed event counts
- get_top_events(start, end, limit?): event types ranked by frequency
- get_funnel(steps, start, end): sequential conversion rates through ordered event steps
- get_retention(cohort_event, return_event, start, end): day-N cohort retention

Today: {today}

Output ONLY valid JSON matching this schema exactly:
{{
  "reasoning": "brief explanation of your approach",
  "steps": [
    {{
      "tool": "tool_name",
      "rationale": "why this tool is needed",
      "args_preview": "human-readable description of the arguments you will pass"
    }}
  ],
  "complexity": "simple|moderate|complex",
  "caveats": "any assumptions or limitations the user should know"
}}"""


class PlannerAgent:
    """Single LLM call that produces a structured query plan.

    Uses RAG to inject relevant project context (models, schema, service code)
    so the plan is grounded in the actual codebase rather than guessing.
    """

    async def plan(self, question: str) -> dict:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        rag_context = rag_service.retrieve(question, top_k=3)

        user_msg = f"Question: {question}"
        if rag_context:
            user_msg += f"\n\nRelevant project context (retrieved from codebase):\n{rag_context}"

        result = await llm_service.chat_json(
            messages=[
                {"role": "system", "content": PLANNER_SYSTEM.format(today=today)},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=600,
        )

        # Guarantee required keys are always present
        result.setdefault("reasoning", "")
        result.setdefault("steps", [])
        result.setdefault("complexity", "unknown")
        result.setdefault("caveats", "")
        return result


planner_agent = PlannerAgent()
