from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class EventTypeSummary(BaseModel):
    event_type: str
    count: int
    unique_users: int
    percentage: float


class AnalyticsSummary(BaseModel):
    tenant_id: UUID
    period_start: datetime
    period_end: datetime
    total_events: int
    unique_users: int
    unique_sessions: int
    event_types: list[EventTypeSummary]


class TimeSeriesPoint(BaseModel):
    timestamp: datetime
    event_count: int
    unique_users: int
    unique_sessions: int


class TimeSeriesResponse(BaseModel):
    tenant_id: UUID
    granularity: Literal["hour", "day", "week"]
    event_type: str | None
    data: list[TimeSeriesPoint]


class FunnelStep(BaseModel):
    event_type: str
    count: int
    conversion_rate: float


class FunnelResponse(BaseModel):
    tenant_id: UUID
    steps: list[FunnelStep]
    total_entered: int
    total_completed: int
    overall_conversion: float


class TopEventEntry(BaseModel):
    event_type: str
    count: int
    unique_users: int
