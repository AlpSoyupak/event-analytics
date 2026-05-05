from dataclasses import dataclass


@dataclass(frozen=True)
class EvalCase:
    id: str
    question: str
    expected_tools: list[str]
    criteria: list[str]


EVAL_CASES: list[EvalCase] = [
    EvalCase(
        id="summary_7d",
        question="Give me a summary of all activity in the last 7 days.",
        expected_tools=["get_summary"],
        criteria=[
            "States the total number of events",
            "States the number of unique users",
            "Explicitly references the 7-day time period",
        ],
    ),
    EvalCase(
        id="top_events",
        question="Which event types occur most often?",
        expected_tools=["get_top_events"],
        criteria=[
            "Names at least 3 specific event types (e.g. page_view, login)",
            "Includes counts or relative frequency for each",
            "Ranks them in order",
        ],
    ),
    EvalCase(
        id="purchase_funnel",
        question="What percentage of users who view a page end up making a purchase?",
        expected_tools=["get_funnel"],
        criteria=[
            "Provides a specific conversion percentage",
            "References both page_view and purchase as funnel steps",
            "States how many users entered the funnel",
        ],
    ),
    EvalCase(
        id="signup_retention",
        question="How well do we retain users after they sign up?",
        expected_tools=["get_retention"],
        criteria=[
            "References signup as the cohort-defining event",
            "Mentions day-N retention rates or a retention trend",
            "Cites at least one specific retention percentage",
        ],
    ),
    EvalCase(
        id="weekly_trend",
        question="Is overall activity trending up or down compared to 14 days ago?",
        expected_tools=["get_timeseries"],
        criteria=[
            "Compares at least two distinct time periods",
            "States a clear direction: up, down, or stable",
            "Supports the claim with specific event counts",
        ],
    ),
]
