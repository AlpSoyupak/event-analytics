import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.agents.analytics_agent import AnalyticsAgent
from app.agents.critic_agent import critic_agent
from app.agents.planner_agent import planner_agent


class OrchestratorAgent:
    """Coordinates three specialised agents in sequence:

    1. PlannerAgent  — decides which analytics tools are needed (uses RAG for context)
    2. AnalystAgent  — executes the agentic tool loop against the real DB
    3. CriticAgent   — reviews the draft answer and corrects it if needed

    The HITL flow splits this into two HTTP calls:
      Phase 1 (POST /ai/plan)           → runs planner only, returns plan for human review
      Phase 2 (POST /ai/execute/{id})   → runs analyst + critic with the approved plan
    """

    def __init__(self, tenant_id: uuid.UUID, db: AsyncSession) -> None:
        self.tenant_id = tenant_id
        self.db = db

    async def run(self, question: str) -> dict:
        """Full pipeline: plan → analyse → critique. No human review step."""
        plan = await planner_agent.plan(question)
        return await self._execute(question, plan)

    async def run_with_plan(self, question: str, plan: dict) -> dict:
        """HITL phase 2: skip planning (already done), run analyst + critic."""
        return await self._execute(question, plan)

    async def _execute(self, question: str, plan: dict) -> dict:
        analyst = AnalyticsAgent(tenant_id=self.tenant_id, db=self.db)
        draft_answer, tools_used = await analyst.run(question)
        critique = await critic_agent.critique(question, draft_answer, tools_used)

        return {
            "answer": critique.get("final_answer", draft_answer),
            "plan": plan,
            "meta": {
                "tools_used": tools_used,
                "critic_approved": critique.get("approved", True),
                "critic_issues": critique.get("issues", []),
            },
        }
