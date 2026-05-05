import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class StepTrace:
    step: str
    duration_ms: float
    skipped: bool = False
    error: str | None = None


@dataclass
class WorkflowResult:
    output: dict
    traces: list[StepTrace] = field(default_factory=list)

    @property
    def total_duration_ms(self) -> float:
        return sum(t.duration_ms for t in self.traces)

    def as_trace_dicts(self) -> list[dict]:
        return [
            {
                "step": t.step,
                "duration_ms": round(t.duration_ms, 1),
                "skipped": t.skipped,
                "error": t.error,
            }
            for t in self.traces
        ]


class WorkflowStep(ABC):
    """Single unit of work in a workflow.

    Receives the shared context dict, returns an updated copy.
    If the step's output is already present in context (e.g. pre-supplied
    for HITL), it should return ctx unchanged — the base class marks it skipped.
    """

    name: str

    @abstractmethod
    async def run(self, ctx: dict) -> dict:
        ...


class Workflow:
    """Runs a sequence of WorkflowSteps, collecting per-step timing traces.

    Context is a plain dict that flows through every step. Steps are free to
    read upstream outputs and write their own keys without interfering with
    each other.
    """

    def __init__(self, steps: list[WorkflowStep]) -> None:
        self.steps = steps

    async def run(self, ctx: dict) -> WorkflowResult:
        traces: list[StepTrace] = []

        for step in self.steps:
            t0 = time.perf_counter()
            try:
                new_ctx = await step.run(ctx)
                elapsed = (time.perf_counter() - t0) * 1000
                skipped = new_ctx is ctx
                traces.append(StepTrace(step.name, elapsed, skipped=skipped))
                ctx = new_ctx
            except Exception as exc:
                elapsed = (time.perf_counter() - t0) * 1000
                traces.append(StepTrace(step.name, elapsed, error=str(exc)))
                raise WorkflowError(step.name, exc) from exc

        return WorkflowResult(output=ctx, traces=traces)


class WorkflowError(Exception):
    def __init__(self, step_name: str, cause: Exception) -> None:
        super().__init__(f"Workflow failed at step '{step_name}': {cause}")
        self.step_name = step_name
        self.cause = cause
