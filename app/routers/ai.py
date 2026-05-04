from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.analytics_agent import AnalyticsAgent
from app.config import get_settings
from app.database import get_db
from app.dependencies.rate_limit import require_tenant
from app.models.tenant import Tenant
from app.services.analytics_service import analytics_service
from app.services.llm_service import llm_service

router = APIRouter(prefix="/ai", tags=["ai"])
settings = get_settings()


class AskRequest(BaseModel):
    question: str = Field(..., min_length=5, max_length=500, examples=["Why did signups drop last week?"])


def _require_groq_key() -> None:
    if not settings.groq_api_key:
        raise HTTPException(
            status_code=503,
            detail="GROQ_API_KEY is not configured. Set it in your .env file.",
        )


@router.post("/ask")
async def ask(
    body: AskRequest,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Ask a natural-language question about your event data.

    The agent will autonomously decide which analytics queries to run,
    chain them together, and return a synthesised answer.

    Example questions:
    - "What were the top events last month?"
    - "Show me the signup → purchase conversion rate for the past 30 days."
    - "Did page views drop at any point in the last two weeks?"
    """
    _require_groq_key()
    agent = AnalyticsAgent(tenant_id=tenant.id, db=db)
    answer = await agent.run(body.question)
    return {"answer": answer}


@router.get("/report")
async def weekly_report(
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Generate a plain-English weekly analytics report for the last 7 days.

    Fetches summary, top events, and daily timeseries data, then asks the LLM
    to write a concise narrative with trends and one actionable recommendation.
    """
    _require_groq_key()
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)

    summary, top_events, timeseries = (
        await analytics_service.get_summary(tenant.id, week_ago, now, db),
        await analytics_service.get_top_events(tenant.id, week_ago, now, 5, db),
        await analytics_service.get_timeseries(tenant.id, week_ago, now, "day", db),
    )

    prompt = f"""\
Generate a concise weekly analytics report (4-6 sentences) for a product team.
Highlight the most important trends, note anything unusual, and end with one
specific, actionable recommendation.

SUMMARY (last 7 days):
{summary}

TOP 5 EVENTS:
{top_events}

DAILY EVENT COUNTS:
{timeseries}

Write in plain English. Be specific with numbers. Do not use bullet points."""

    report = await llm_service.chat(
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a concise analytics reporter writing for a non-technical product team. "
                    "Use plain English, cite specific numbers, and keep the report under 150 words."
                ),
            },
            {"role": "user", "content": prompt},
        ]
    )

    return {
        "period_start": week_ago.isoformat(),
        "period_end": now.isoformat(),
        "report": report,
        "data": {
            "summary": summary,
            "top_events": top_events,
        },
    }
