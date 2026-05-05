from app.agents.analytics_agent import AnalyticsAgent
from app.agents.critic_agent import critic_agent
from app.agents.planner_agent import planner_agent
from app.workflows.base import Workflow, WorkflowStep


class PlanStep(WorkflowStep):
    """Call the planner to decide which tools to use.

    Skipped if 'plan' is already in context — this is how HITL phase 2
    re-uses a plan the human already approved.
    """

    name = "planner"

    async def run(self, ctx: dict) -> dict:
        if "plan" in ctx:
            return ctx  # signals Workflow to mark this step skipped
        plan = await planner_agent.plan(ctx["question"])
        return {**ctx, "plan": plan}


class AnalyseStep(WorkflowStep):
    """Run the agentic tool loop against the real database."""

    name = "analyst"

    async def run(self, ctx: dict) -> dict:
        agent = AnalyticsAgent(tenant_id=ctx["tenant_id"], db=ctx["db"])
        draft_answer, tools_used = await agent.run(ctx["question"])
        return {**ctx, "draft_answer": draft_answer, "tools_used": tools_used}


class CritiqueStep(WorkflowStep):
    """Review the analyst's draft and rewrite it if it falls short."""

    name = "critic"

    async def run(self, ctx: dict) -> dict:
        result = await critic_agent.critique(
            question=ctx["question"],
            draft_answer=ctx["draft_answer"],
            tools_used=ctx["tools_used"],
        )
        return {**ctx, "critique": result}


# Shared singleton — stateless, safe to reuse across requests
analytics_workflow = Workflow([PlanStep(), AnalyseStep(), CritiqueStep()])
