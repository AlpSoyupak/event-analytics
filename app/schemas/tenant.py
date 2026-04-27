from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

PLAN_LIMITS: dict[str, dict] = {
    "free":       {"rate_limit_per_minute": 100,    "max_events_per_day": 10_000},
    "starter":    {"rate_limit_per_minute": 500,    "max_events_per_day": 100_000},
    "pro":        {"rate_limit_per_minute": 2_000,  "max_events_per_day": 1_000_000},
    "enterprise": {"rate_limit_per_minute": 10_000, "max_events_per_day": 100_000_000},
}


class TenantCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    plan: Literal["free", "starter", "pro", "enterprise"] = "free"


class TenantUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=255)
    plan: Literal["free", "starter", "pro", "enterprise"] | None = None
    is_active: bool | None = None


class TenantResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    api_key: str
    plan: str
    rate_limit_per_minute: int
    max_events_per_day: int
    is_active: bool
    created_at: datetime
