import logging
from contextlib import contextmanager
from dataclasses import dataclass
from time import perf_counter
from typing import Callable, Dict, Iterator, List


@dataclass(frozen=True)
class SummaryTimingStep:
    name: str
    elapsed_seconds: float


class DailySummaryTimingProfiler:
    """Collects named timing sections for daily summary generation."""

    def __init__(self, clock: Callable[[], float] = perf_counter):
        self._clock = clock
        self._started_at = clock()
        self.steps: List[SummaryTimingStep] = []

    @contextmanager
    def step(self, name: str) -> Iterator[None]:
        started_at = self._clock()
        try:
            yield
        finally:
            elapsed = self._clock() - started_at
            self.steps.append(SummaryTimingStep(name, elapsed))

    @property
    def total_seconds(self) -> float:
        return self._clock() - self._started_at

    def summary_by_name(self) -> Dict[str, float]:
        summary: Dict[str, float] = {}
        for step in self.steps:
            summary[step.name] = summary.get(step.name, 0.0) + step.elapsed_seconds
        return summary

    def top_steps(self, limit: int = 2) -> List[SummaryTimingStep]:
        return [
            SummaryTimingStep(name, elapsed)
            for name, elapsed in sorted(
                self.summary_by_name().items(),
                key=lambda item: item[1],
                reverse=True,
            )[:limit]
        ]

    def format_report(self, status: str, top_limit: int = 2) -> str:
        timings = self.summary_by_name()
        all_steps = ", ".join(
            f"{name}={elapsed:.3f}s"
            for name, elapsed in sorted(timings.items())
        )
        top_steps = ", ".join(
            f"{step.name}={step.elapsed_seconds:.3f}s"
            for step in self.top_steps(top_limit)
        )
        return (
            f"Daily summary timing status={status} "
            f"total={self.total_seconds:.3f}s "
            f"top_{top_limit}=[{top_steps}] "
            f"steps=[{all_steps}]"
        )

    def log_report(self, logger: logging.Logger, status: str) -> None:
        logger.info(self.format_report(status))
