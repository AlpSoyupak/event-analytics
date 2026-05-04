from app.services.llm_service import llm_service

CRITIC_SYSTEM = """\
You are a QA agent reviewing a draft answer produced by an analytics assistant.

Evaluate the draft on four criteria:
1. Relevance — does it directly address the question?
2. Specificity — does it cite concrete numbers from the data, not vague generalities?
3. Tool appropriateness — were the right tools called for this question type?
4. Logical soundness — are there contradictions or calculation errors?

Output ONLY valid JSON matching this schema exactly:
{{
  "approved": true,
  "issues": [],
  "final_answer": "copy the draft verbatim if approved; rewrite it here if not"
}}

If the draft is acceptable, set approved=true, issues=[], and copy the draft into final_answer.
If it has problems, set approved=false, list each issue, and write a corrected answer in final_answer.
final_answer must always be non-empty — it is what gets returned to the user."""


class CriticAgent:
    """Single LLM call that reviews and optionally rewrites the analyst's draft answer.

    The critic receives the question, the tools used, and the draft. It either
    approves the draft or produces a corrected final_answer.
    """

    async def critique(
        self,
        question: str,
        draft_answer: str,
        tools_used: list[str],
    ) -> dict:
        user_msg = (
            f"Question: {question}\n\n"
            f"Tools called by analyst: {', '.join(tools_used) if tools_used else 'none'}\n\n"
            f"Draft answer:\n{draft_answer}"
        )

        result = await llm_service.chat_json(
            messages=[
                {"role": "system", "content": CRITIC_SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=1024,
        )

        result.setdefault("approved", True)
        result.setdefault("issues", [])
        # Always fall back to the original draft so final_answer is never empty
        if not result.get("final_answer"):
            result["final_answer"] = draft_answer
        return result


critic_agent = CriticAgent()
