import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.analytics_agent import AnalyticsAgent
from app.evaluation.cases import EVAL_CASES, EvalCase
from app.evaluation.judge import llm_judge
from app.services.cache_service import cache_service

_RESULTS_TTL = 60 * 60 * 24 * 7  # keep latest results for 7 days


class EvalRunner:
    """Runs every EvalCase against the live agent, judges each answer, and
    persists an aggregated report to Redis.

    Designed to be called from both the HTTP endpoint (for on-demand runs)
    and the Celery task (for scheduled nightly runs).
    """

    async def run(self, tenant_id: uuid.UUID, db: AsyncSession) -> dict:
        case_results = []

        for case in EVAL_CASES:
            case_result = await self._run_case(case, tenant_id, db)
            case_results.append(case_result)

        scored = [r for r in case_results if r["score"] is not None]
        overall = (
            sum(r["score"]["overall_score"] for r in scored) / len(scored)
            if scored else 0.0
        )

        report = {
            "tenant_id": str(tenant_id),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_score": round(overall, 3),
            "cases_run": len(case_results),
            "cases_passed": sum(
                1 for r in scored if r["score"]["overall_score"] >= 0.8
            ),
            "cases": case_results,
        }

        await cache_service.set(
            f"ai:eval:latest:{tenant_id}",
            report,
            ttl=_RESULTS_TTL,
        )
        return report

    async def _run_case(self, case: EvalCase, tenant_id: uuid.UUID, db: AsyncSession) -> dict:
        try:
            agent = AnalyticsAgent(tenant_id=tenant_id, db=db)
            answer, tools_used = await agent.run(case.question)
            score = await llm_judge.evaluate(case, answer, tools_used)
        except Exception as exc:
            return {
                "case_id": case.id,
                "question": case.question,
                "answer": None,
                "tools_used": [],
                "score": None,
                "error": str(exc),
            }

        return {
            "case_id": case.id,
            "question": case.question,
            "answer": answer,
            "tools_used": tools_used,
            "score": score,
            "error": None,
        }


eval_runner = EvalRunner()
