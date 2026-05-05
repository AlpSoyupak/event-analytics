from app.evaluation.cases import EvalCase
from app.services.llm_service import llm_service

JUDGE_SYSTEM = """\
You are an impartial evaluator for an analytics AI assistant.

You will receive:
- The question the user asked
- The tools the agent called
- The expected tools for this question type
- The agent's answer
- A list of criteria the answer must satisfy

Score each criterion as met (true) or not met (false) with a one-sentence reason.
Also check whether the agent called the expected tools.

Output ONLY valid JSON:
{{
  "criteria_scores": [
    {{"criterion": "...", "met": true, "reason": "one sentence"}}
  ],
  "tool_coverage": true,
  "tool_coverage_reason": "brief explanation",
  "overall_score": 0.85
}}

overall_score = (criteria met / total criteria). If tool_coverage is false, cap overall_score at 0.5."""


class LLMJudge:
    """Uses the LLM itself to score agent answers against predefined criteria.

    This is the 'LLM-as-judge' evaluation pattern: a separate LLM call
    evaluates the quality of another LLM call's output, providing structured
    scores that can be tracked over time.
    """

    async def evaluate(
        self,
        case: EvalCase,
        answer: str,
        tools_used: list[str],
    ) -> dict:
        user_msg = (
            f"Question: {case.question}\n\n"
            f"Expected tools: {', '.join(case.expected_tools)}\n"
            f"Tools actually called: {', '.join(tools_used) if tools_used else 'none'}\n\n"
            f"Agent's answer:\n{answer}\n\n"
            f"Criteria to evaluate:\n"
            + "\n".join(f"- {c}" for c in case.criteria)
        )

        result = await llm_service.chat_json(
            messages=[
                {"role": "system", "content": JUDGE_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=800,
        )

        result.setdefault("criteria_scores", [])
        result.setdefault("tool_coverage", False)
        result.setdefault("tool_coverage_reason", "")
        result.setdefault("overall_score", 0.0)
        return result


llm_judge = LLMJudge()
