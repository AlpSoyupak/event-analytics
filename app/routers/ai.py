import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.langchain_analytics_agent import LangChainAnalyticsAgent
from app.agents.orchestrator import OrchestratorAgent
from app.agents.planner_agent import planner_agent
from app.config import get_settings
from app.database import get_db
from app.dependencies.rate_limit import require_tenant
from app.evaluation.runner import eval_runner
from app.models.tenant import Tenant
from app.services.analytics_service import analytics_service
from app.services.cache_service import cache_service
from app.services.llm_service import llm_service
from app.services.vector_rag_service import vector_rag_service

router = APIRouter(prefix="/ai", tags=["ai"])
settings = get_settings()

_PLAN_TTL = 300  # seconds a pending plan stays valid in Redis


class AskRequest(BaseModel):
    question: str = Field(..., min_length=5, max_length=500, examples=["Why did signups drop last week?"])


def _require_groq_key() -> None:
    if not settings.groq_api_key:
        raise HTTPException(
            status_code=503,
            detail="GROQ_API_KEY is not configured. Set it in your .env file.",
        )


# ---------------------------------------------------------------------------
# POST /ai/ask  —  fast path (no human review)
# ---------------------------------------------------------------------------

@router.post("/ask")
async def ask(
    body: AskRequest,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Ask a natural-language question about your event data.

    Runs the full multi-agent pipeline automatically:
    **Planner** (RAG-aware) → **Analyst** (tool loop) → **Critic** (review & revise).

    Use `/ai/plan` + `/ai/execute/{plan_id}` if you want to review the query plan
    before it runs (Human-in-the-Loop flow).
    """
    _require_groq_key()
    orchestrator = OrchestratorAgent(tenant_id=tenant.id, db=db)
    return await orchestrator.run(body.question)


# ---------------------------------------------------------------------------
# HITL Phase 1 — POST /ai/plan
# ---------------------------------------------------------------------------

@router.post("/plan")
async def create_plan(
    body: AskRequest,
    tenant: Tenant = Depends(require_tenant),
):
    """
    **HITL Phase 1** — Generate a query plan without executing it.

    The planner uses RAG to retrieve relevant project context (models, service code)
    before deciding which analytics tools to call and in what order.

    Returns a `plan_id` and the full plan for human review.
    Call `POST /ai/execute/{plan_id}` to approve and run it.
    The plan expires after 5 minutes.
    """
    _require_groq_key()
    plan = await planner_agent.plan(body.question)
    plan_id = str(uuid.uuid4())

    await cache_service.set(
        f"ai:plan:{plan_id}",
        {"question": body.question, "plan": plan, "tenant_id": str(tenant.id)},
        ttl=_PLAN_TTL,
    )

    return {
        "plan_id": plan_id,
        "expires_in_seconds": _PLAN_TTL,
        "question": body.question,
        "plan": plan,
        "next_step": f"POST /api/v1/ai/execute/{plan_id}",
    }


# ---------------------------------------------------------------------------
# HITL Phase 2 — POST /ai/execute/{plan_id}
# ---------------------------------------------------------------------------

@router.post("/execute/{plan_id}")
async def execute_plan(
    plan_id: str,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    **HITL Phase 2** — Execute a previously reviewed plan.

    Retrieves the plan created by `POST /ai/plan`, runs the analyst tool loop,
    then passes the draft answer through the critic for review and correction.

    The plan is deleted from Redis after execution (single-use).
    Returns 404 if the plan has expired or already been executed.
    """
    _require_groq_key()
    stored = await cache_service.get(f"ai:plan:{plan_id}")
    if not stored:
        raise HTTPException(status_code=404, detail="Plan not found or expired.")

    if stored.get("tenant_id") != str(tenant.id):
        raise HTTPException(status_code=403, detail="Plan belongs to a different tenant.")

    await cache_service.delete(f"ai:plan:{plan_id}")

    orchestrator = OrchestratorAgent(tenant_id=tenant.id, db=db)
    return await orchestrator.run_with_plan(
        question=stored["question"],
        plan=stored["plan"],
    )


# ---------------------------------------------------------------------------
# POST /ai/eval/run  —  trigger evaluation pipeline
# ---------------------------------------------------------------------------

@router.post("/eval/run")
async def run_evaluation(
    background_tasks: BackgroundTasks,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Trigger the agent evaluation pipeline against your live event data.

    Runs all 5 predefined test cases through the agent, then scores each
    answer using an LLM judge (LLM-as-judge pattern).

    **Synchronous** — runs in the request for immediate results.
    To schedule nightly runs via Celery, dispatch `run_agent_evaluation.delay(tenant_id)`.

    Returns overall score (0–1) and per-case breakdowns.
    """
    _require_groq_key()
    report = await eval_runner.run(tenant_id=tenant.id, db=db)
    return report


# ---------------------------------------------------------------------------
# GET /ai/eval/results  —  fetch latest stored results
# ---------------------------------------------------------------------------

@router.get("/eval/results")
async def get_eval_results(
    tenant: Tenant = Depends(require_tenant),
):
    """
    Fetch the most recent evaluation report for this tenant.

    Results are stored in Redis after each `POST /ai/eval/run` call
    (or after a Celery `run_agent_evaluation` task completes).
    Returns 404 if no evaluation has been run yet.
    """
    report = await cache_service.get(f"ai:eval:latest:{tenant.id}")
    if not report:
        raise HTTPException(
            status_code=404,
            detail="No evaluation results found. Run POST /api/v1/ai/eval/run first.",
        )
    return report


# ---------------------------------------------------------------------------
# GET /ai/report  —  automated weekly narrative
# ---------------------------------------------------------------------------

@router.get("/report")
async def weekly_report(
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Generate a plain-English weekly analytics report for the last 7 days.

    Fetches summary, top events, and daily timeseries, then asks the LLM to write
    a concise narrative with trends and one actionable recommendation.
    """
    _require_groq_key()
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)

    summary = await analytics_service.get_summary(tenant.id, week_ago, now, db)
    top_events = await analytics_service.get_top_events(tenant.id, week_ago, now, 5, db)
    timeseries = await analytics_service.get_timeseries(tenant.id, week_ago, now, "day", db)

    prompt = (
        "Generate a concise weekly analytics report (4-6 sentences) for a product team.\n"
        "Highlight the most important trends, note anything unusual, and end with one\n"
        "specific, actionable recommendation.\n\n"
        f"SUMMARY (last 7 days):\n{summary}\n\n"
        f"TOP 5 EVENTS:\n{top_events}\n\n"
        f"DAILY EVENT COUNTS:\n{timeseries}\n\n"
        "Write in plain English. Be specific with numbers. Do not use bullet points."
    )

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


# ---------------------------------------------------------------------------
# POST /ai/langchain/ask  —  LangChain tool-calling agent
# ---------------------------------------------------------------------------

@router.post("/langchain/ask")
async def langchain_ask(
    body: AskRequest,
    tenant: Tenant = Depends(require_tenant),
    db: AsyncSession = Depends(get_db),
):
    """
    Ask a question using the **LangChain-powered** analytics agent.

    Uses `ChatGroq` from `langchain-groq` and five `StructuredTool` wrappers
    around the same analytics_service functions, orchestrated by LangChain's
    `AgentExecutor` (LCEL under the hood) instead of the raw Groq SDK.

    Returns the same `answer` + `meta.tools_used` shape as `/ai/ask`.
    """
    _require_groq_key()
    agent = LangChainAnalyticsAgent(tenant_id=tenant.id, db=db)
    answer, tools_used = await agent.run(body.question)
    return {"answer": answer, "meta": {"tools_used": tools_used, "agent": "langchain"}}


# ---------------------------------------------------------------------------
# POST /ai/langchain/index  —  build FAISS semantic vector index
# ---------------------------------------------------------------------------

@router.post("/langchain/index")
async def build_vector_index():
    """
    Build (or rebuild) the **FAISS semantic vector index** over project source files.

    Chunks every `.py` file by function/class boundaries, embeds each chunk with
    `sentence-transformers/all-MiniLM-L6-v2` (~90 MB, downloaded on first call),
    and stores the index in memory for the lifetime of the process.

    Once built, the index can be queried programmatically via `vector_rag_service.retrieve()`.
    Re-call after code changes to keep the index current.
    """
    count = vector_rag_service.build_index()
    return {"indexed_chunks": count, "embedding_model": "all-MiniLM-L6-v2"}
