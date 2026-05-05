import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.workflows.analytics_workflow import analytics_workflow


class OrchestratorAgent:
    """Thin coordinator that runs the analytics workflow and shapes its output.

    The workflow handles the planner → analyst → critic sequence with per-step
    timing traces. The orchestrator's only job is to supply the initial context
    and format the WorkflowResult for the HTTP response.

    Two entry points:
    - run()            full pipeline (plan + analyse + critique)
    - run_with_plan()  HITL phase 2 — plan already approved, skip PlanStep
    """

    def __init__(self, tenant_id: uuid.UUID, db: AsyncSession) -> None:
        self.tenant_id = tenant_id
        self.db = db

    async def run(self, question: str) -> dict:
        result = await analytics_workflow.run({
            "question": question,
            "tenant_id": self.tenant_id,
            "db": self.db,
        })
        return self._format(result)

    async def run_with_plan(self, question: str, plan: dict) -> dict:
        result = await analytics_workflow.run({
            "question": question,
            "plan": plan,          # PlanStep detects this and skips
            "tenant_id": self.tenant_id,
            "db": self.db,
        })
        return self._format(result)

    @staticmethod
    def _format(result) -> dict:
        ctx = result.output
        critique = ctx.get("critique", {})
        return {
            "answer": critique.get("final_answer", ctx.get("draft_answer", "")),
            "plan": ctx.get("plan", {}),
            "meta": {
                "tools_used": ctx.get("tools_used", []),
                "critic_approved": critique.get("approved", True),
                "critic_issues": critique.get("issues", []),
                "workflow_trace": result.as_trace_dicts(),
                "total_duration_ms": round(result.total_duration_ms, 1),
            },
        }
