from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EventCreate(BaseModel):
    event_type: str = Field(..., min_length=1, max_length=255)
    user_id: str | None = Field(None, max_length=255)
    session_id: str | None = Field(None, max_length=255)
    source: str = Field("api", max_length=100)
    properties: dict[str, Any] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(default_factory=dict)
    received_at: datetime | None = Field(
        None,
        description="Override ingestion timestamp (e.g. for backfill). Defaults to server time.",
    )


class EventBatchCreate(BaseModel):
    events: list[EventCreate] = Field(..., min_length=1, max_length=1000)


class EventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    tenant_id: UUID
    event_type: str
    user_id: str | None
    session_id: str | None
    source: str
    properties: dict[str, Any]
    meta: dict[str, Any]
    received_at: datetime
    processed_at: datetime | None


class EventIngestResponse(BaseModel):
    event_id: UUID
    status: str = "accepted"
    queued_at: datetime


class BatchIngestResponse(BaseModel):
    accepted: int
    status: str = "accepted"


class ReplayRequest(BaseModel):
    start: datetime
    end: datetime
    event_type: str | None = Field(None, description="Filter by event type. Omit to replay all types.")


class ReplayJobResponse(BaseModel):
    job_id: str
    status: str = "queued"
